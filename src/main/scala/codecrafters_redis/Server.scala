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
import codecrafters_redis.utils.EncodingType
import java.util.concurrent.{ScheduledExecutorService, Executors}
import java.util.concurrent.TimeUnit
import scala.util.matching.Regex
import java.util.regex.Pattern
import scala.jdk.CollectionConverters._
import java.nio.file.Files
import java.nio.file.Paths

case class Event(outputStream: OutputStream, message: ArrayBuffer[String])

case class CacheElement(value: String, expiry: Option[Long], setAt: LocalDateTime)

case class Config(dir: String, dbFileName: String)

object Server {
    
    final val cache = new ConcurrentHashMap[String, CacheElement]()
    final var config = new Config("", "")

    private val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

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

    private def saveState(): Unit = {
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

        val rdbDecoder = new RDBParserEncoder()

        println(s"Reading file: ${file.getAbsolutePath()}")
        println(s"File exists: ${file.exists()}")
        println(s"File length: ${file.length()}")
        
        val bytes: Array[Byte] = Files.readAllBytes(Paths.get(config.dir, config.dbFileName))
        val hexString = bytes.map("%02x".format(_)).mkString("\n")
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
                println(redisVersionKey)
                println(redisVersionValue)
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

                println(totalCount)
                println(expiryCount)
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

                println(s"${key} ${value}")

                cache.put(key, new CacheElement(value, Some(expiry), setAt))
            }
            else if ((bytes(idx).toLong & 0xFF) == 0x00 && totalCount > 0) {
                idx += 1
                totalCount -= 1

                val (key, newIndex1) = rdbDecoder.string_decoder(bytes, idx)
                idx = newIndex1

                val (value, newIndex2) = rdbDecoder.string_decoder(bytes, idx)
                idx = newIndex2

                print(s"${key} ${value}")
                cache.put(key, new CacheElement(value, None, LocalDateTime.now()))
            } else {
                idx += 1
            }
        }
    }

    def globToRegex(glob: String): String = {
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

    private def processEvent(event: Event): Unit = {
        println(s"Proccessing event: ${event}")
        if (event.message(0).toUpperCase() == "PING") {
            event.outputStream.write("+PONG\r\n".getBytes())

        } else if (event.message(0).toUpperCase() == "ECHO") {
            if (event.message.length < 2) {

            }
            event.outputStream.write(s"+${event.message(1)}\r\n".getBytes())

        } else if (event.message(0).toUpperCase() == "SET") {
            if (event.message.length < 3) {
                throw new Exception("Invalid arguments")
            }

            var exp: Option[Long] = None
            if (event.message.length >= 5 && event.message(3).toUpperCase() == "PX") {
                exp = Some(event.message(4).toLong)
            }

            cache.put(event.message(1), new CacheElement(event.message(2), exp, LocalDateTime.now()))
            event.outputStream.write("+OK\r\n".getBytes())
        } else if (event.message(0).toUpperCase() == "GET") {
            if (event.message.length < 2) {
                throw new Exception("Invalid arguments")
            }

            if (cache.containsKey(event.message(1))) {
                val value = cache.get(event.message(1))
                
                value.expiry match {
                    case Some(exp) => {
                        val duration = Duration.between(value.setAt, LocalDateTime.now()).toMillis.toLong
                        if (duration <= exp) {
                            event.outputStream.write(s"+${value.value}\r\n".getBytes())
                        } else {
                            cache.remove(event.message(1))
                            event.outputStream.write("$-1\r\n".getBytes())
                        }
                    }
                    case None => event.outputStream.write(s"+${value.value}\r\n".getBytes())
                }
            } else {
                event.outputStream.write("$-1\r\n".getBytes())
            }
        } else if (event.message(0).toUpperCase() == "CONFIG" && event.message(1).toUpperCase() == "GET") {
            if (event.message.length != 3) {
                throw new Exception("Invalid arguments")
            }

            if (event.message(2) == "dir") {
                event.outputStream.write(("*2\r\n$3\r\ndir\r\n$" + config.dir.length + "\r\n" + config.dir + "\r\n").getBytes())
            } else if (event.message(2) == "dbfilename") {
                event.outputStream.write(("*2\r\n$10\r\ndbfilename\r\n$" + config.dbFileName.length + "\r\n" + config.dbFileName + "\r\n").getBytes())
            }
        } else if (event.message(0).toUpperCase() == "SAVE") {
            saveState()
        } else if (event.message(0).toUpperCase() == "KEYS") {
            if (event.message.length != 2) {
                throw new Exception("Invalid arguments, required: KEYS <PATTERN>")
            }
            val pattern: Regex = globToRegex(event.message(1)).r
            println(s"pattern: ${pattern}")
            
            val filteredKeys = cache.keySet().asScala.filter { key =>
                // Use proper regex matching syntax
                pattern.findFirstIn(key).isDefined
            }

            val len = filteredKeys.size
            var output = s"*${len}\r\n"
            for (key <- filteredKeys) {
                output += s"$$${key.length}\r\n"
                output += s"${key}\r\n"
            }

            event.outputStream.write(output.getBytes())
        }

        event.outputStream.flush()
    }

    private def eventLoop(queue: BlockingQueue[Event]): Unit = {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                val event = queue.take()
                processEvent(event)
            } catch {
                case _: InterruptedException => {
                    Thread.currentThread().interrupt()
                    return
                }
                case e: Exception => println(s"Error: ${e.getMessage()}")
            }
        }
    }

    def main(args: Array[String]): Unit = {
        val serverSocket = new ServerSocket();
        serverSocket.bind(new InetSocketAddress("localhost", 6379))

        var argMap = parseArguments(args)
        config = new Config(argMap.get("dir").getOrElse("/temp/redis-files/"), argMap.get("dbfilename").getOrElse("dump.rdb"))

        try {
            loadSavedState()
        } catch {
            case e: Exception => println(s"Error while loading saved state: ${e.getMessage()}")
            cache.clear()
        }
        

        var threads = ListBuffer[Thread]()

        var q: BlockingQueue[Event] = new LinkedBlockingQueue[Event]()

        val workerThread = new Thread(() => {
            eventLoop(q)
        })
        workerThread.setDaemon(true)
        workerThread.start()

        while (true) {
            val clientSocket = serverSocket.accept()

            val thread = new Thread(() => {         
                Using.resources(clientSocket.getInputStream(), clientSocket.getOutputStream()) { (is, os) =>
                    val reader = new BufferedReader(new InputStreamReader(is));
                    
                    var command: ArrayBuffer[String] = ArrayBuffer[String]()
                    var idx = 0
                    var len = 0

                    reader.lines().forEach { line =>
                        println(s"Input Line: ${line}")
                        if (line.startsWith("*") && idx >= (2 * len)) {
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
                                    q.offer(new Event(os, command))
                                }
                            }
                        }
                    }
                }
            })
            thread.start()
            threads += thread
        }
        
        threads.foreach(_.join()) 
    }

    private def parseArguments(args: Array[String]): Map[String, String] = {
        args.sliding(2, 2).collect {
            case Array(flag, value) if flag.startsWith("--") => 
                flag.drop(2) -> value
        }.toMap
    }
}