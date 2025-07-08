package codecrafters_redis.utils

import java.io.OutputStream
import java.util.concurrent.ConcurrentHashMap
import _root_.codecrafters_redis.CacheElement
import java.time.LocalDateTime
import codecrafters_redis.Config
import codecrafters_redis.Server.saveState
import scala.util.matching.Regex
import scala.jdk.CollectionConverters._
import java.time.Duration
import java.io.File
import java.nio.file.Files
import scala.collection.mutable.ArrayBuffer
import scala.annotation.switch
import scala.collection.mutable.Set
import java.net.Socket
import java.io.PrintStream
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.Queue
import scala.collection.mutable

class EventProcessor(
    val outputStream: Option[OutputStream],
    val cache: ConcurrentHashMap[String, CacheElement],
    val streamCache: ConcurrentHashMap[String, ConcurrentHashMap[String, ConcurrentHashMap[String, String]]],
    val config: Config,
    val slaveOutputStreams: Set[OutputStream],
    val writeToOutput: Boolean = true,
    val numReplicasWrite: AtomicInteger,
    val unprocessedWrite: AtomicBoolean,
    val lastXADDTime: AtomicLong,
    val lastXADDId: AtomicReference[String],
) {

    final val respEncoder = new RESPEncoder()
    final val compulsoryWrite = Set[String]("REPLCONF")
    final var totalBytesProcessed: Long = 0
    final var eventQueue: mutable.Queue[Array[String]] = mutable.Queue()
    final var multiEnabled: Boolean = false
    final var execOutput: ArrayBuffer[String] = new ArrayBuffer[String]()

    // Helper to convert glob input into compatible regex
    private def glob_to_regex(glob: String): String = {
        val escaped = glob
            .replace("\\", "\\\\")    // Escape backslashes first
            .replace(".", "\\.")      // Escape dots
            .replace("^", "\\^")      // Escape carets
            .replace("$", "\\$")      // Escape dollars
            .replace("+", "\\+")      // Escape plus
            .replace("(", "\\(")      // Escape parentheses
            .replace(")", "\\)")
            .replace("[", "\\[")      // Escape brackets
            .replace("]", "\\]")
            .replace("{", "\\{")      // Escape braces
            .replace("}", "\\}")
            .replace("|", "\\|")      // Escape pipes
            .replace("*", ".*")       // Convert glob * to regex .*
            .replace("?", ".")        // Convert glob ? to regex .
        
        s"^${escaped}$$"
    }

    private def writeToOutput(data: String, command: String, addToArray: Boolean = false): Unit = {
        if (!writeToOutput && !compulsoryWrite.contains(command)) {
            return;
        }

        if (addToArray) {
            execOutput += data
            return
        }

        outputStream match {
            case Some(os) => {
                try {
                    os.write(data.getBytes())
                } catch {
                    case e: Exception => println(s"Error when outputing command: ${command}, error: ${e.getMessage()}")
                }
            }
            case _ => // Do nothing
        }
    }
    // For raw bytes (e.g. RDB file transfer)
    private def writeRawToOutput(data: Array[Byte], command: String): Unit = {
        if (!writeToOutput && !compulsoryWrite.contains(command)) {
            return;
        }
        outputStream match {
            case Some(os) => {
                try {
                    os.write(data)
                } catch {
                    case e: Exception => println(s"Error when outputing command: ${command}, error: ${e.getMessage()}")
                }
            }
            case _ => // Do nothing
        }
    }

    private def validate_stream_key(streamKey: String, current: String): String = {
        var currentKey = current

        if (currentKey == "0-0") {
            throw new Exception("ERR The ID specified in XADD must be greater than 0-0")
        }

        if (currentKey == "*") {
            val currentTime = System.currentTimeMillis()
            currentKey = s"${currentTime}-*"
        }
        println(s"currentKey: ${currentKey}")
        val time = currentKey.split("-")(0)
        val idx = currentKey.split("-")(1)

        idx match {
            case "*" => {
                if (!streamCache.containsKey(streamKey)) {
                    if (time.toLong == 0) {
                        return s"${time}-1"
                    }
                    return s"${time}-0"
                }

                var maxIdx = -1
                val keyIterator = streamCache.get(streamKey).keySet().iterator()
                while (keyIterator.hasNext) {
                    val key = keyIterator.next()

                    val keyTime = key.split("-")(0).toLong
                    val keyIdx = key.split("-")(1).toInt

                    if (keyTime > time.toLong) {
                        throw new Exception("ERR The ID specified in XADD is equal or smaller than the target stream top item")
                    }

                    if (keyTime == time.toLong) {
                        maxIdx = Math.max(keyIdx, maxIdx)
                    }
                }

                return s"${time}-${maxIdx + 1}"
            }
            case _ => {
                if (!streamCache.containsKey(streamKey)) {
                    return currentKey
                }
                val keyIterator = streamCache.get(streamKey).keySet().iterator()
                while (keyIterator.hasNext()) {
                    val key = keyIterator.next()

                    val keyTime = key.split("-")(0).toLong
                    val keyIdx = key.split("-")(1).toInt

                    if (keyTime > time.toLong) {
                        throw new Exception("ERR The ID specified in XADD is equal or smaller than the target stream top item")
                    }

                    if (keyTime == time.toLong && keyIdx >= idx.toInt) {
                        throw new Exception("ERR The ID specified in XADD is equal or smaller than the target stream top item")
                    }
                }

                return currentKey
            }
        }
    }

    private def hash_map_to_array(input: ConcurrentHashMap[String, _]): Array[Any] = {
        val resArray = new ArrayBuffer[Any]()
        val entryIterator = input.entrySet().iterator()
        while (entryIterator.hasNext()) {
            val entry = entryIterator.next()
            resArray.append(entry.getKey())

            entry.getValue() match {
                case s: String => resArray.append(s)
                case map: ConcurrentHashMap[_, _] => resArray.append(hash_map_to_array(map.asInstanceOf[ConcurrentHashMap[String, _]]))
            }
        }

        return resArray.toArray
    }

    private def find_stream_enteries(key: String, start: String, end: String): Array[Any] = {
        var resultMap = new ArrayBuffer[Any]()

        if (!streamCache.containsKey(key)) {
            return resultMap.toArray
        }

        val startTime = start.split("-")(0).toLong
        val startIdx = start.split("-")(1).toInt

        val endTime = end.split("-")(0).toLong
        val endIdx = end.split("-")(1).toInt

        val entryIterator = streamCache.get(key).entrySet().iterator()
        while (entryIterator.hasNext) {
            val entry = entryIterator.next()
            println(s"entry: ${entry.getKey()} ${entry.getValue()}")

            val currTime = entry.getKey().split("-")(0).toLong
            val currIdx = entry.getKey().split("-")(1).toInt
            println(s"currTime: ${currTime}, currIdx: ${currIdx}")
            if ((currTime > startTime || (currTime == startTime && currIdx >= startIdx)) && (currTime < endTime || (currTime == endTime && currIdx <= endIdx))) {
                println(s"entry: ${entry.getKey()}, value: ${entry.getValue()}")
                resultMap.append(Array(entry.getKey(), hash_map_to_array(entry.getValue())))
            }
        }

        return resultMap.toArray
    }

    def process_event(event: Array[String], addToArray: Boolean = false): Unit = {
        if (event.length == 0) {
            throw new Exception("Empty event")
        }

        if (event(0) != "EXEC" && multiEnabled) {
            eventQueue += event
            writeToOutput(respEncoder.encodeSimpleString("QUEUED"), "")
            return
        }

        val command = event(0).toUpperCase()
        command match {
            case "PING" => process_ping(event, addToArray)
            case "ECHO" => process_echo(event, addToArray)
            case "SET" => process_set(event, addToArray)
            case "GET" => process_get(event, addToArray)
            case "CONFIG" => process_config(event, addToArray)
            case "SAVE" => process_save(event, addToArray)
            case "KEYS" => process_keys(event, addToArray)
            case "INFO" => process_info(event, addToArray)
            case "REPLCONF" => process_replconf(event, addToArray)
            case "PSYNC" => process_psync(event, addToArray)
            case "WAIT" => process_wait(event, addToArray)
            case "TYPE" => process_type(event, addToArray)
            case "XADD" => process_xadd(event, addToArray)
            case "XRANGE" => process_xrange(event, addToArray)
            case "XREAD" => process_xread(event, addToArray)
            case "INCR" => process_incr(event, addToArray)
            case "MULTI" => process_multi(event, addToArray)
            case "EXEC" => process_exec(event, addToArray)
            case _ => throw new Exception("Unsupported command")
        }

        totalBytesProcessed += respEncoder.encodeArray(event.asInstanceOf[Array[Any]]).getBytes().length.toLong
    }

    private def propogate_command(event: Array[String]): Unit = {
        for (slaveOutputStream <- slaveOutputStreams) {
            try {
                slaveOutputStream.write(respEncoder.encodeArray(event.asInstanceOf[Array[Any]]).getBytes())
            } catch {
                case e: Exception => println(s"Error while writing to replica: ${e.getMessage()}")
            }
        }
    }

    private def process_ping(event: Array[String], addToArray: Boolean = false): Unit = {
        writeToOutput(respEncoder.encodeSimpleString("PONG"), event(0), addToArray)
    }

    private def process_echo(event: Array[String], addToArray: Boolean = false): Unit = {
        if (event.length != 2) {
            throw new Exception("Invalid Inputs, required: ECHO <command>")
        }

        writeToOutput(respEncoder.encodeSimpleString(event(1)), event(0), addToArray)
    }

    private def process_set(event: Array[String], addToArray: Boolean = false): Unit = {
        println(s"event: ${event}")
        if (event.length != 3 && event.length != 5) {
            throw new Exception("Invalid Inputs, required: SET <key> <value> (optional) PX <expiry>")
        }

        var exp: Option[Long] = None
        // Check if expiry is provided
        if (event.length == 5 && event(3).toUpperCase() != "PX") {
            throw new Exception("Invalid Inputs, required: SET <key> <value> (optional) PX <expiry>")
        } else if (event.length == 5) {
            exp = Some(event(4).toLong)
        }

        val key = event(1)
        val value = event(2)
        cache.put(key, new CacheElement(value, "string", exp, LocalDateTime.now()))

        writeToOutput(respEncoder.encodeSimpleString("OK"), event(0), addToArray)
        unprocessedWrite.set(true)
        if (config.role == "master")    propogate_command(event)
    }

    private def process_get(event: Array[String], addToArray: Boolean = false): Unit = {
        if (event.length != 2) {
            throw new Exception("Invalid Inputs, required: GET <key>")
        }

        val key = event(1)
        // Check if cache contains key
        if (cache.containsKey(key)) {
            val value = cache.get(key)
            
            // Check if expiry exists
            value.expiry match {
                case Some(exp) => {
                    val duration = Duration.between(value.setAt, LocalDateTime.now()).toMillis.toLong
                    // Check if element is expired
                    if (duration <= exp) {
                        writeToOutput(respEncoder.encodeSimpleString(value.value), event(0), addToArray)
                    } else {
                        // Remove if expired
                        cache.remove(key)
                        writeToOutput(respEncoder.encodeBulkString(""), event(0), addToArray)
                    }
                }
                case None => writeToOutput(respEncoder.encodeSimpleString(value.value), event(0), addToArray)
            }
        } else {
            writeToOutput(respEncoder.encodeBulkString(""), event(0), addToArray)
        }
    }

    private def process_config(event: Array[String], addToArray: Boolean = false): Unit = {
        if (event.length != 3 || event(1).toUpperCase() != "GET") {
            throw new Exception("Invalid Inputs, required: CONFIG GET <key>")
        }

        event(2) match {
            case "dir" => writeToOutput(respEncoder.encodeArray(Array("dir", config.dir)), event(0), addToArray)
            case "dbfilename" => writeToOutput(respEncoder.encodeArray(Array("dbfilename", config.dbFileName)), event(0), addToArray)
            case _ => throw new Exception("Invalid Inputs, required: key = dir/dbfilename")
        }
    }

    private def process_save(event: Array[String], addToArray: Boolean = false): Unit = {
        // Save current cache state to RDB File
        saveState()
        writeToOutput(respEncoder.encodeSimpleString("OK"), event(0), addToArray)
    }

    private def process_keys(event: Array[String], addToArray: Boolean = false): Unit = {
        if (event.length != 2) {
            throw new Exception("Invalid Inputs, required: KEYS <pattern>")
        }

        // Convert glob input into valid regex string, then convert string to regex
        val pattern: Regex = glob_to_regex(event(1)).r

        // Filter keys based on which match with given pattern
        val filteredKeys = cache.keySet().asScala.filter { key =>
            // Use proper regex matching syntax
            pattern.findFirstIn(key).isDefined
        }
        writeToOutput(respEncoder.encodeArray(filteredKeys.toArray), event(0), addToArray)
    }

    private def process_info(event: Array[String], addToArray: Boolean = false): Unit = {
        writeToOutput(
            respEncoder.encodeBulkString(
                s"role:${config.role}\n" +
                s"master_replid:${config.master_replid}\n" +
                s"master_repl_offset:${config.master_repl_offset}"
            ), event(0), addToArray)
    }

    private def process_replconf(event: Array[String], addToArray: Boolean = false): Unit = {
        if (event.length != 3) {
            throw new Exception("Invalid Inputs, required: REPLCONF <arg1> <arg2>")
        }



        event(1) match {
            case "listening-port" => {
                try {
                    slaveOutputStreams += outputStream.get
                    writeToOutput(respEncoder.encodeSimpleString("OK"), event(0), addToArray)
                } catch {
                    case e: Exception => {
                        writeToOutput(respEncoder.encodeSimpleString(e.getMessage()), event(0), addToArray)
                    }
                }

                return
            }
            case "GETACK" => {
                writeToOutput(respEncoder.encodeArray(Array("REPLCONF", "ACK", totalBytesProcessed.toString)), event(0), addToArray)
                return
            }
            case "ACK" => {
                println(s"Sent ack: ${numReplicasWrite.get()}")
                numReplicasWrite.incrementAndGet()
                println(s"Recieved ack: ${numReplicasWrite.get()}, ${System.currentTimeMillis()}")
                return
            }
            case _ => {
                writeToOutput(respEncoder.encodeSimpleString("OK"), event(0), addToArray)
            }
        }
    }

    private def process_psync(event: Array[String], addToArray: Boolean = false): Unit = {
        if (event.length != 3) {
            throw new Exception("Invalid Inputs, required: PSYNC ? -1")
        }

        writeToOutput(respEncoder.encodeSimpleString(s"FULLRESYNC ${config.master_replid} ${config.master_repl_offset}"), event(0), addToArray)
        saveState()
        
        val dir = new File(config.dir)
        val file = new File(dir, config.dbFileName)
        val bytes = Files.readAllBytes(file.toPath)
        writeRawToOutput(s"$$${bytes.length}\r\n".getBytes(), event(0))
        writeRawToOutput(bytes, event(0))
    }

    private def process_wait(event: Array[String], addToArray: Boolean = false): Unit = {
        if (event.length != 3) {
            throw new Exception("Invalid Inputs, required: WAIT 0 60000")
        }

        val requiredReplicas = event(1).toInt
        val requiredTimeout = event(2).toLong

        numReplicasWrite.set(0)
        if (unprocessedWrite.get()) propogate_command(Array("REPLCONF", "GETACK", "*"))
        else    numReplicasWrite.set(slaveOutputStreams.size)

        val start = System.currentTimeMillis()
        while (numReplicasWrite.get() < requiredReplicas && (System.currentTimeMillis() - start) < requiredTimeout) {
            Thread.sleep(10)
        }
        println("time: ", System.currentTimeMillis())
        println("Time taken: ", (System.currentTimeMillis() - start))
        writeToOutput(respEncoder.encodeInteger(numReplicasWrite.get()), event(0), addToArray)
    }

    private def process_type(event: Array[String], addToArray: Boolean = false): Unit = {
        if (event.length != 2) {
            throw new Exception("Invalid Inputs, required: TYPE <key>")
        }

        val key = event(1)
        if (cache.containsKey(key)) {
            writeToOutput(respEncoder.encodeSimpleString(cache.get(key).valueType), event(0), addToArray)
        } else {
            writeToOutput(respEncoder.encodeSimpleString("none"), event(0), addToArray)
        }
    }

    private def process_xadd(event: Array[String], addToArray: Boolean = false): Unit = {
        if (event.length < 5 || event.length % 2 == 0) {
            throw new Exception("Invalid Inputs, required: XADD <stream-key> <current-key> key value")
        }

        val streamKey = event(1)
        val currentKey = validate_stream_key(streamKey, event(2))

        if (!cache.containsKey(streamKey)) {
            cache.put(streamKey, new CacheElement("", "stream", None, LocalDateTime.now()))
        }

        if (cache.get(streamKey).valueType != "stream") {
            writeToOutput(respEncoder.encodeSimpleString("-1"), event(0), addToArray)
            return
        }

        if (!streamCache.containsKey(streamKey)) {
            streamCache.put(streamKey, new ConcurrentHashMap())
        }

        if (!streamCache.get(streamKey).contains(currentKey)) {
            streamCache.get(streamKey).put(currentKey, new ConcurrentHashMap())
        }

        var idx = 3
        while (idx < event.length) {
            streamCache.get(streamKey).get(currentKey).put(event(idx), event(idx + 1))
            idx = idx + 2
        }

        lastXADDTime.set(System.currentTimeMillis())
        lastXADDId.set(currentKey)
        writeToOutput(respEncoder.encodeBulkString(currentKey), event(0), addToArray)
    }

    private def process_xrange(event: Array[String], addToArray: Boolean = false): Unit = {
        if (event.length != 4) {
            throw new Exception("Invalid Inputs, required: XRANGE <key> <start> <end>")
        }

        val key = event(1)
        var start = event(2)
        var end = event(3)

        if (start != "-" && !start.contains("-")) {
            start += "-0"
        }

        if (start == "-") {
            start = "0-0"
        }

        if (end != "+" && !end.contains("-")) {
            end += "-0"
        }

        if (end == "+") {
            end = s"${Long.MaxValue}-${Int.MaxValue}"
        }

        val resultMap = find_stream_enteries(key, start, end)
        writeToOutput(respEncoder.encodeArray(resultMap), event(0), addToArray)
    }

    private def process_xread(event: Array[String], addToArray: Boolean = false): Unit = {
        if (event.length < 4 || event.length % 2 != 0) {
            throw new Exception("Invalid Inputs, required: XREAD streams <stream_key> <id>")
        }

        var idx = 2
        var numStreams = (event.length - 2) / 2

        val last = lastXADDId.get()

        if (event(1) == "block") {
            val timeOut = event(2).toLong
            val start = System.currentTimeMillis()
            
            if (timeOut == 0) {
                var len = 0
                while (start > lastXADDTime.get() && (start - lastXADDTime.get()) < 10000) {
                    Thread.sleep(10)
                }
            } else {
                while ((System.currentTimeMillis() - start) < timeOut) {
                    Thread.sleep(5)
                }
            }

            idx = 4
            numStreams = (event.length - 4) / 2
        }

        val resMap = new ArrayBuffer[Any]
        var isEmpty = true
        while ((idx + numStreams) < event.length) {
            var start: String = event(idx + numStreams)
            if (start != "$" && start != "-" && !start.contains("-")) {
                start += "-0"
            }

            if (start == "-") {
                start = "0-0"
            }

            if (start == "$") {
                start = last
            }

            val time = start.split("-")(0)
            val currIdx = start.split("-")(1).toInt
            println(s"key: ${time}-${currIdx + 1}")
            val currMap = find_stream_enteries(event(idx), s"${time}-${currIdx + 1}", s"${Long.MaxValue}-${Int.MaxValue}")
            println(s"currMap: ${currMap}")
            resMap.append(Array(event(idx), currMap))
            if (currMap.length > 0) {
                isEmpty = false
            }
            idx += 1
        }
        
        if (isEmpty) {
            writeToOutput(respEncoder.encodeBulkString(""), event(0), addToArray)
            return
        }

        writeToOutput(respEncoder.encodeArray(resMap.toArray), event(0), addToArray)
    }

    private def process_incr(event: Array[String], addToArray: Boolean = false): Unit = {
        if (event.length != 2) {
            throw new Exception("Invalid Inputs, required: INCR <key>")
        }

        val key = event(1)
        if (cache.containsKey(key)) {
            try {
                val value = cache.get(key).value.toInt + 1
                cache.put(key, new CacheElement(value.toString, "string", cache.get(key).expiry, cache.get(key).setAt))

                writeToOutput(respEncoder.encodeInteger(value), event(0), addToArray)
            } catch {
                case _: Throwable => throw new Exception("ERR value is not an integer or out of range")
            }
        } else {
            cache.put(key, new CacheElement("1", "string", None, LocalDateTime.now()))

            writeToOutput(respEncoder.encodeInteger(1), event(0), addToArray)
        }
    }

    private def process_multi(event: Array[String], addToArray: Boolean = false): Unit = {
        multiEnabled = true
        println(s"mutli: ${Thread.currentThread().getId()}")
        writeToOutput(respEncoder.encodeSimpleString("OK"), event(0), addToArray)
    }

    private def process_exec(event: Array[String], addToArray: Boolean = false): Unit = {
        println(s"exec: ${Thread.currentThread().getId()}")
        if (!multiEnabled) {
            throw new Exception("ERR EXEC without MULTI")
        }

        multiEnabled = false
        println(s"event empty; ${eventQueue.isEmpty}")
        execOutput.clear()
        if (eventQueue.isEmpty) {
            writeToOutput(respEncoder.encodeArray(Array()), event(0), false)
            return
        }

        while (!eventQueue.isEmpty) {
            val event = eventQueue.dequeue()
            process_event(event, true)
        }

        writeToOutput(respEncoder.encodeArray(execOutput.toArray, true), event(0), addToArray)
    }

    private def process_discard(event: Array[String], addToArray: Boolean = false): Unit = {
        if (!multiEnabled)  throw new Exception("ERR DISCARD without MULTI")

        eventQueue.clear()
        writeToOutput(respEncoder.encodeSimpleString("OK"), event(0), addToArray)
    }
}