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

class EventProcessor(
    val outputStream: Option[OutputStream],
    val cache: ConcurrentHashMap[String, CacheElement],
    val streamCache: ConcurrentHashMap[String, ConcurrentHashMap[String, ConcurrentHashMap[String, String]]],
    val config: Config,
    val slaveOutputStreams: Set[OutputStream],
    val writeToOutput: Boolean = true,
    val numReplicasWrite: AtomicInteger,
    val unprocessedWrite: AtomicBoolean
) {

    final val respEncoder = new RESPEncoder()
    final val compulsoryWrite = Set[String]("REPLCONF")
    final var totalBytesProcessed: Long = 0

    def start_processing(): Unit = {

    }

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

    private def writeToOutput(data: Array[Byte], command: String): Unit = {
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

    private def validate_stream_key(streamKey: String, currentKey: String): String = {
        val time = currentKey.split("-")(0)
        val idx = currentKey.split("-")(1)

        if (currentKey == "0-0") {
            throw new Exception("ERR The ID specified in XADD must be greater than 0-0")
        }

        time match {
            case "*" => {
                return currentKey
            }
            case _ => {
                idx match {
                    case "*" => {
                        if (!streamCache.containsKey(streamKey)) {
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
                        println(s"time: ${time.toLong}")
                        println(s"maxIdx: ${maxIdx}")
                        if (time.toLong == 0 && maxIdx == -1) {
                            return s"${time}-1"
                        }
                        return s"${time}-${maxIdx + 1}"
                    }
                    case _ => {
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
        }
    }

    def process_event(event: Array[String]): Unit = {
        if (event.length == 0) {
            throw new Exception("Empty event")
        }

        val command = event(0).toUpperCase()
        command match {
            case "PING" => process_ping(event)
            case "ECHO" => process_echo(event)
            case "SET" => process_set(event)
            case "GET" => process_get(event)
            case "CONFIG" => process_config(event)
            case "SAVE" => process_save(event)
            case "KEYS" => process_keys(event)
            case "INFO" => process_info(event)
            case "REPLCONF" => process_replconf(event)
            case "PSYNC" => process_psync(event)
            case "WAIT" => process_wait(event)
            case "TYPE" => process_type(event)
            case "XADD" => process_xadd(event)
            case _ => throw new Exception("Unsupported command")
        }

        totalBytesProcessed += respEncoder.encodeArray(event).getBytes().length.toLong
    }

    private def propogate_command(event: Array[String]): Unit = {
        for (slaveOutputStream <- slaveOutputStreams) {
            try {
                slaveOutputStream.write(respEncoder.encodeArray(event).getBytes())
            } catch {
                case e: Exception => println(s"Error while writing to replica: ${e.getMessage()}")
            }
        }
    }

    private def process_ping(event: Array[String]): Unit = {
        writeToOutput(respEncoder.encodeSimpleString("PONG").getBytes(), event(0))
    }

    private def process_echo(event: Array[String]): Unit = {
        if (event.length != 2) {
            throw new Exception("Invalid Inputs, required: ECHO <command>")
        }

        writeToOutput(respEncoder.encodeSimpleString(event(1)).getBytes(), event(0))
    }

    private def process_set(event: Array[String]): Unit = {
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

        writeToOutput(respEncoder.encodeSimpleString("OK").getBytes(), event(0))
        unprocessedWrite.set(true)
        if (config.role == "master")    propogate_command(event)
    }

    private def process_get(event: Array[String]): Unit = {
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
                        writeToOutput(respEncoder.encodeSimpleString(value.value).getBytes(), event(0))
                    } else {
                        // Remove if expired
                        cache.remove(key)
                        writeToOutput(respEncoder.encodeBulkString("").getBytes(), event(0))
                    }
                }
                case None => writeToOutput(respEncoder.encodeSimpleString(value.value).getBytes(), event(0))
            }
        } else {
            writeToOutput(respEncoder.encodeBulkString("").getBytes(), event(0))
        }
    }

    private def process_config(event: Array[String]): Unit = {
        if (event.length != 3 || event(1).toUpperCase() != "GET") {
            throw new Exception("Invalid Inputs, required: CONFIG GET <key>")
        }

        event(2) match {
            case "dir" => writeToOutput(respEncoder.encodeArray(Array("dir", config.dir)).getBytes(), event(0))
            case "dbfilename" => writeToOutput(respEncoder.encodeArray(Array("dbfilename", config.dbFileName)).getBytes(), event(0))
            case _ => throw new Exception("Invalid Inputs, required: key = dir/dbfilename")
        }
    }

    private def process_save(event: Array[String]): Unit = {
        // Save current cache state to RDB File
        saveState()
        writeToOutput(respEncoder.encodeSimpleString("OK").getBytes(), event(0))
    }

    private def process_keys(event: Array[String]): Unit = {
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
        writeToOutput(respEncoder.encodeArray(filteredKeys.toArray).getBytes(), event(0))
    }

    private def process_info(event: Array[String]): Unit = {
        writeToOutput(
            respEncoder.encodeBulkString(
                s"role:${config.role}\n" +
                s"master_replid:${config.master_replid}\n" +
                s"master_repl_offset:${config.master_repl_offset}"
            ).getBytes(), event(0))
    }

    private def process_replconf(event: Array[String]): Unit = {
        if (event.length != 3) {
            throw new Exception("Invalid Inputs, required: REPLCONF <arg1> <arg2>")
        }

        event(1) match {
            case "listening-port" => {
                try {
                    slaveOutputStreams += outputStream.get
                    writeToOutput(respEncoder.encodeSimpleString("OK").getBytes(), event(0))
                } catch {
                    case e: Exception => {
                        writeToOutput(respEncoder.encodeSimpleString(e.getMessage()).getBytes(), event(0))
                    }
                }

                return
            }
            case "GETACK" => {
                writeToOutput(respEncoder.encodeArray(Array("REPLCONF", "ACK", totalBytesProcessed.toString)).getBytes(), event(0))
                return
            }
            case "ACK" => {
                println(s"Sent ack: ${numReplicasWrite.get()}")
                numReplicasWrite.incrementAndGet()
                println(s"Recieved ack: ${numReplicasWrite.get()}, ${System.currentTimeMillis()}")
                return
            }
            case _ => {
                writeToOutput(respEncoder.encodeSimpleString("OK").getBytes(), event(0))
            }
        }
    }

    private def process_psync(event: Array[String]): Unit = {
        if (event.length != 3) {
            throw new Exception("Invalid Inputs, required: PSYNC ? -1")
        }

        writeToOutput(respEncoder.encodeSimpleString(s"FULLRESYNC ${config.master_replid} ${config.master_repl_offset}").getBytes(), event(0))
        saveState()
        
        val dir = new File(config.dir)
        val file = new File(dir, config.dbFileName)
        val bytes = Files.readAllBytes(file.toPath)
        writeToOutput(s"$$${bytes.length}\r\n".getBytes(), event(0))
        writeToOutput(bytes, event(0))
    }

    private def process_wait(event: Array[String]): Unit = {
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
        writeToOutput(respEncoder.encodeInteger(numReplicasWrite.get()).getBytes(), event(0))
    }

    private def process_type(event: Array[String]): Unit = {
        if (event.length != 2) {
            throw new Exception("Invalid Inputs, required: TYPE <key>")
        }

        val key = event(1)
        if (cache.containsKey(key)) {
            writeToOutput(respEncoder.encodeSimpleString(cache.get(key).valueType).getBytes(), event(0))
        } else {
            writeToOutput(respEncoder.encodeSimpleString("none").getBytes(), event(0))
        }
    }

    private def process_xadd(event: Array[String]): Unit = {
        if (event.length < 5 || event.length % 2 == 0) {
            throw new Exception("Invalid Inputs, required: XADD <stream-key> <current-key> key value")
        }

        val streamKey = event(1)
        val currentKey = validate_stream_key(streamKey, event(2))

        if (!cache.containsKey(streamKey)) {
            cache.put(streamKey, new CacheElement("", "stream", None, LocalDateTime.now()))
        }

        if (cache.get(streamKey).valueType != "stream") {
            writeToOutput(respEncoder.encodeSimpleString("-1").getBytes(), event(0))
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

        writeToOutput(respEncoder.encodeSimpleString(currentKey).getBytes(), event(0))
    }
}