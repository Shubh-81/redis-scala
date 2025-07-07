package codecrafters_redis

import java.net._;
import java.io._;
import scala.util.Using
import scala.collection.mutable.ListBuffer
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.ConcurrentHashMap
import java.time.LocalDateTime
import java.time.Duration
import scala.collection.immutable.Map
import scala.io.Source
import codecrafters_redis.utils.RDBParserEncoder
import java.util.concurrent.{ScheduledExecutorService, Executors}
import java.util.concurrent.TimeUnit
import scala.util.matching.Regex
import java.util.regex.Pattern
import scala.jdk.CollectionConverters._
import java.nio.file.Files
import java.nio.file.Paths
import codecrafters_redis.utils.RESPEncoder
import codecrafters_redis.utils.RESPDecoder
import scala.util.Random
import codecrafters_redis.utils.EventProcessor
import scala.collection.mutable.Set
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

case class Event(eventProcessor: EventProcessor, message: ArrayBuffer[String])
case class CacheElement(value: String, valueType: String = "string", expiry: Option[Long], setAt: LocalDateTime)
case class Config(dir: String, dbFileName: String, role: String, master_replid: String, master_repl_offset: Long, master_host: String, master_port: Int, port: Int, host: String)

object Server {
    
    final val cache = new ConcurrentHashMap[String, CacheElement]()
    final val streamCache = new ConcurrentHashMap[String, ConcurrentHashMap[String, ConcurrentHashMap[String, String]]]()
    final var config = new Config("", "", "master", Random.alphanumeric.take(40).mkString, 0, "", 6379, 6379, "")
    final val respEncoder = new RESPEncoder()
    final val respDecoder = new RESPDecoder()
    private val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
    final var slaveOutputStreams = Set[OutputStream]()
    final val q: BlockingQueue[Event] = new LinkedBlockingQueue[Event]()
    final var numReplicasWrite = new AtomicInteger(0)
    final var unprocessedWrite = new AtomicBoolean(false)
    final var lastXADDTime = new AtomicLong(0)
    final var lastXADDId = new AtomicReference[String]("0-0")
    final var multiEnabled = new AtomicBoolean(false)

    private def startScheduledSaveState(): Unit = {
        scheduler.scheduleAtFixedRate(
            new Runnable {
                def run(): Unit = {
                    try {
                        saveState()
                        println(s"Saved current state at: ${LocalDateTime.now()}")
                    } catch {
                        case e: Exception => println(s"Error in scheduled saveState: ${e.getMessage()}")
                    }
                }
            },
            30,
            30,
            TimeUnit.SECONDS
        )
    }

    def saveState(): Unit = {
        val dir = new File(config.dir)
        if (!dir.exists()) {
            println(s"${config.dir} does not exists, creating new directory")
            dir.mkdirs()
        }

        val file = new File(dir, config.dbFileName)
        if (!file.exists()) {
            println(s"${config.dbFileName} does not exists, creating new file")
            file.createNewFile()
        }

        var totalCount = 0
        var expiryCount = 0

        val snapshot = new java.util.HashMap[String, CacheElement](cache)

        snapshot.forEach { (key, value) => 
            value.expiry match {
                case Some(exp) => {
                    val duration = Duration.between(value.setAt, LocalDateTime.now()).toMillis()
                    if (duration <= exp) {
                        totalCount += 1
                        expiryCount += 1
                    } else {
                        snapshot.remove(key)
                    }
                }
                case None => {
                    totalCount += 1
                }
            }
        }

        val rdbParser = new RDBParserEncoder()
        val outputStream = new FileOutputStream(file)

        outputStream.write(rdbParser.string_encoder("REDIS0011", false))

        outputStream.write(0xFA.toByte)
        outputStream.write(rdbParser.string_encoder("redis-ver"))
        outputStream.write(rdbParser.string_encoder("6.0.16"))

        outputStream.write(0xFE.toByte)
        outputStream.write(0x00.toByte)

        outputStream.write(0xFB.toByte)
        outputStream.write(rdbParser.size_encoder(totalCount))
        outputStream.write(rdbParser.size_encoder(expiryCount))

        snapshot.forEach { (key, value) =>
            value.expiry match {
                case Some(exp) => {
                    outputStream.write(0xFC.toByte)
                    outputStream.write(rdbParser.expiry_encoder(exp, value.setAt))
                }
                case None => ()
            }

            outputStream.write(0x00.toByte)
            outputStream.write(rdbParser.string_encoder(key))
            outputStream.write(rdbParser.string_encoder(value.value))
        }

        outputStream.write(0xFF.toByte)
        outputStream.close()
    }

    private def loadFromBytes(bytes: Array[Byte]): Unit = {
        val rdbDecoder = new RDBParserEncoder()

        var idx = 9
        var totalCount = 0L
        var expiryCount = 0L

        while (idx < bytes.length) {
            if ((bytes(idx).toLong & 0xFF) == 0xFA) {
                idx += 1
                val (redisVersionKey, newIndex1) = rdbDecoder.string_decoder(bytes, idx)
                idx = newIndex1
                val (redisVersionValue, newIndex2) = rdbDecoder.string_decoder(bytes, idx)
                idx = newIndex2
            }
            else if ((bytes(idx).toLong & 0xFF) == 0xFE) {
                idx += 2
            }
            else if ((bytes(idx).toLong & 0xFF) == 0xFB) {
                idx += 1

                val (totalCountVal, newIndex1) = rdbDecoder.size_decoder(bytes, idx)
                idx = newIndex1
                totalCount = totalCountVal

                val (expiryCountVal, newIndex2) = rdbDecoder.size_decoder(bytes, idx)
                idx = newIndex2
                expiryCount = expiryCountVal
            }
            else if ((bytes(idx).toLong & 0xFF) == 0xFC && expiryCount > 0 && totalCount > 0) {
                idx += 1
                expiryCount -= 1
                totalCount -= 1

                val (expiry, setAt, newIndex1) = rdbDecoder.expiry_decoder(bytes, idx)
                idx = newIndex1

                idx += 1
                val (key, newIndex2) = rdbDecoder.string_decoder(bytes, idx)
                idx = newIndex2

                val (value, newIndex3) = rdbDecoder.string_decoder(bytes, idx)
                idx = newIndex3

                cache.put(key, new CacheElement(value, "string", Some(expiry), setAt))
            }
            else if ((bytes(idx).toLong & 0xFF) == 0x00 && totalCount > 0) {
                idx += 1
                totalCount -= 1

                val (key, newIndex1) = rdbDecoder.string_decoder(bytes, idx)
                idx = newIndex1

                val (value, newIndex2) = rdbDecoder.string_decoder(bytes, idx)
                idx = newIndex2

                cache.put(key, new CacheElement(value, "string", None, LocalDateTime.now()))
            } else {
                idx+=1
            }
        }
    }

    private def loadSavedState(): Unit = {
        val dir = new File(config.dir)
        if (!dir.exists()) {
            println(s"${config.dir} does not exists")
            return
        }
        val file = new File(dir, config.dbFileName)
        if (!file.exists()) {
            println(s"${config.dbFileName} does not exists")
            return
        }
        
        val bytes: Array[Byte] = Files.readAllBytes(Paths.get(config.dir, config.dbFileName))
        loadFromBytes(bytes)
    }

    private def eventLoop(queue: BlockingQueue[Event]): Unit = {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                val event = queue.take()
                event.eventProcessor.process_event(event.message.toArray)
            } catch {
                case _: InterruptedException => {
                    Thread.currentThread().interrupt()
                    return
                }
                case e: Exception => println(s"Error: ${e.getMessage()}")
            }
        }
    }

    private def handshake(masterHost: String, masterPort: String, slavePort: Int) {
        val socket = new Socket(masterHost, masterPort.toInt)
        Using.resources(socket.getInputStream(), socket.getOutputStream()) { (in, os) =>
            val out = new PrintStream(os, true)
            val reader = new BufferedReader(new InputStreamReader(in))

            out.print(respEncoder.encodeArray(Array("PING")))
            var response = respDecoder.decodeSimpleString(reader.readLine())

            if (response != "PONG") {
                throw new Exception(s"Error establishing connection with master, expected: PONG, recieved: ${response}")
            }

            out.print(respEncoder.encodeArray(Array("REPLCONF", "listening-port", slavePort.toString)))
            response = respDecoder.decodeSimpleString(reader.readLine())

            if (response != "OK") {
                throw new Exception(s"Error establishing connection with master, expected: OK (1), recieved: ${response}")
            } 

            out.print(respEncoder.encodeArray(Array("REPLCONF", "capa", "psync2")))
            response = respDecoder.decodeSimpleString(reader.readLine())

            if (response != "OK") {
                throw new Exception(s"Error establishing connection with master, expected: OK (2), recieved: ${response}")
            } 

            out.print(respEncoder.encodeArray(Array("PSYNC", "?", "-1")))
            val masterConfig = respDecoder.decodeSimpleString(reader.readLine()).split(" ")
            val fileLen = respDecoder.decodeSimpleString(reader.readLine()).toInt

            val fileBytes = new ArrayBuffer[Byte](fileLen)
            var bytesRead = 0
            while (reader.ready() && bytesRead < fileLen) {
                val byte = reader.read().toByte
                fileBytes += byte
                bytesRead += 1
            }

            loadFromBytes(fileBytes.toArray)

            val eventProcessor = new EventProcessor(Some(os), cache, streamCache, config, slaveOutputStreams, writeToOutput = false, numReplicasWrite, unprocessedWrite, lastXADDTime, lastXADDId, multiEnabled)
            var command: ArrayBuffer[String] = ArrayBuffer[String]()
            var idx = 0
            var len = 0

            while (true) {
                reader.lines().forEach { line =>
                    println(s"processed: ${eventProcessor.totalBytesProcessed}(s) bytes")
                    if (idx == (2 * len)) {
                        if (line(0) == '*') len = Integer.parseInt(line.substring(1))
                        else    len = Integer.parseInt(line)
                        idx = 0

                        command = ArrayBuffer[String]()
                    } else {
                        if (idx % 2 == 0) {
                            idx += 1
                        } else {
                            command += line
                            idx += 1

                            if (idx == 2 * len) {
                                println(s"command: ${command}")
                                q.offer(new Event(eventProcessor, command))
                            }
                        }
                    }
                }
            }
        }
    }

    private def set_config(args: Array[String]): Unit = {
        var argMap = parseArguments(args)

        // Get RDB File location from arguments or revert to defaults
        val dir = argMap.get("dir").getOrElse("./test/dir/")
        val dbFileName = argMap.get("dbfilename").getOrElse("test.rdb")

        // Get host and port
        val port = argMap.get("port").getOrElse("6379").toInt
        val host = argMap.get("host").getOrElse("localhost")

        // Get master/slave config
        val isSlave = argMap.contains("replicaof")
        val role = if (isSlave) "slave" else "master"

        var masterHost = host
        var masterPort = port

        // If is slave then get master details and handshake
        if (isSlave) {
            val Array(masterHostStr, masterPortStr) = argMap.get("replicaof").getOrElse(" ").split(" ")
            masterHost = masterHostStr
            masterPort = masterPortStr.toInt
        }

        config = new Config(
            dir,
            dbFileName,
            role, 
            Random.alphanumeric.take(40).mkString, 
            0,
            masterHost,
            masterPort,
            port,
            host
        )
    }

    def main(args: Array[String]): Unit = {
        set_config(args)

        // Connect to provided host and port
        val serverSocket = new ServerSocket()
        println(s"Server started as host: ${config.host}, port: ${config.port}")
        serverSocket.bind(new InetSocketAddress(config.host, config.port))

        // Try to load saved RDB
        try {
            loadSavedState()
        } catch {
            case e: Exception => println(s"Error while loading saved state: ${e.getMessage()}")
            cache.clear()
        }
        
        // Buffer to store threads
        var threads = ListBuffer[Thread]()
        
        val workerThread = new Thread(() => {
            eventLoop(q)
        })
        workerThread.setDaemon(true)
        workerThread.start()

        if (config.role == "slave") {
            var slaveThread = new Thread(() => {
                handshake(config.master_host, config.master_port.toString(), config.port)
            })

            slaveThread.setDaemon(true)
            slaveThread.start()
        }

        while (true) {
            val clientSocket = serverSocket.accept()
            val thread = new Thread(() => {
                try {
                    Using.resources(clientSocket.getOutputStream(), new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) { (os, reader) =>
                        val eventProcessor = new EventProcessor(Some(os), cache, streamCache, config, slaveOutputStreams, writeToOutput = true, numReplicasWrite, unprocessedWrite, lastXADDTime, lastXADDId, multiEnabled)
                        var command: ArrayBuffer[String] = ArrayBuffer[String]()
                        var idx = 0
                        var len = 0

                        var line: String = null
                        while ({
                            line = reader.readLine()
                            line != null
                        }) {
                            if (idx >= (2 * len)) {
                                len = Integer.parseInt(line.substring(1))
                                idx = 0

                                command = ArrayBuffer[String]()
                            } else {
                                if (idx % 2 == 0) {
                                    idx += 1
                                } else {
                                    command += line
                                    idx += 1

                                    if (idx == 2 * len) {
                                        println(s"command: ${command}")
                                        try {
                                            eventProcessor.process_event(command.toArray)
                                        } catch {
                                            case e: Exception => {
                                                println(s"Error while processing event: ${e.getMessage()}")
                                                os.write(respEncoder.encodeSimpleError(e.getMessage()).getBytes())
                                            }
                                        }
                                        
                                    }
                                }
                            }
                        }
                    }
                    } catch {
                        case e: Exception => { 
                            println(s"Error in thread: ${Thread.currentThread()}, error: ${e.getMessage()}")
                            e.printStackTrace()
                        }
                    }
            })
            thread.start()   
            threads += thread
        }

        workerThread.join()
        for (thread <- threads) thread.join()
    }

    private def parseArguments(args: Array[String]): Map[String, String] = {
        args.sliding(2, 2).collect {
            case Array(flag, value) if flag.startsWith("--") => 
                flag.drop(2) -> value
        }.toMap
    }
}