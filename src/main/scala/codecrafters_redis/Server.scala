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

        Using(new FileWriter(file)) { writer => 

            // Header
            writer.write("52 45 44 49 53 30 30 31 31  // Magic string + version number (ASCII): \"REDIS0011\".\n")

            // Metadata
            writer.write("FA                             // Indicates the start of a metadata subsection.\n")
            writer.write(s"${rdbParser.encode("redis-ver", EncodingType.STRING_ENCODING)}  // The name of the metadata attribute (string encoded): \"redis-ver\".\n")
            writer.write(s"${rdbParser.encode("6.0.16", EncodingType.STRING_ENCODING)}           // The value of the metadata attribute (string encoded): \"6.0.16\".\n")

            // Database Details
            writer.write("FE                       // Indicates the start of a database subsection.\n")
            writer.write("00                       /* The index of the database (size encoded). Here, the index is 0. */\n")

            // Hash Table Info
            writer.write("FB                       // Indicates that hash table size information follows.\n")
            writer.write(s"${rdbParser.size_encoder(totalCount)} // Size of hash table\n")
            writer.write(s"${rdbParser.size_encoder(expiryCount)}  // Size of hash table storing expiry\n")

            // Storing elemnts
            snapshot.forEach { (key, value) => 
                value.expiry match {
                    case Some(exp) => {
                        writer.write("FC           // Indicates key has an expiry in milliseconds\n")
                        writer.write(s"${rdbParser.expiry_encoder(exp, value.setAt)} // Expiry timestamp in unix time\n")
                    }
                    case None => ()
                }
                writer.write("00                       // Value type is string.\n")
                writer.write(s"${rdbParser.encode(key, EncodingType.STRING_ENCODING)}\n")
                writer.write(s"${rdbParser.encode(value.value, EncodingType.STRING_ENCODING)}\n")
            }

            // End of file
            writer.write("FF                      // Indicates file is ending and checksum follows\n")
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

        val rdbDecoder = new RDBParserEncoder()

        Using(Source.fromFile(file)) { source =>
            val lines = source.getLines().toArray
            var i = 1

            var totalCount: Long = 0
            var expiryCount: Long = 0
            while (i < lines.length) {
                val line = lines(i).replaceAll("""//.*|/\*[^*]*\*/""", "").trim

                if (line == "FA") {
                    // Skip metdata information
                    i += 3
                } 
                else if (line == "FE") {
                    // Support for single db currently
                    i += 2
                } 
                else if (line == "FB") {
                    if (lines.length < (i + 3)) {
                        throw new Exception("Invalid RDB file format")
                    }
                    totalCount = rdbDecoder.size_decoder(lines(i + 1).replaceAll("""//.*|/\*[^*]*\*/""", "").trim)
                    expiryCount = rdbDecoder.size_decoder(lines(i + 2).replaceAll("""//.*|/\*[^*]*\*/""", "").trim)
                    i += 3
                }
                else if (line == "FC" && totalCount > 0 && expiryCount > 0) {
                    if (lines.length < (i + 5)) {
                        throw new Exception("Invalid RDB format")
                    }
                    totalCount -= 1
                    expiryCount -= 1

                    val expiryInfo = rdbDecoder.expiry_decoder(lines(i + 1).replaceAll("""//.*|/\*[^*]*\*/""", "").trim)
                    if (expiryInfo.expiry > 0) {
                        val key = rdbDecoder.string_decoder(lines(i + 2).replaceAll("""//.*|/\*[^*]*\*/""", "").trim)
                        val value = rdbDecoder.string_decoder(lines(i + 3).replaceAll("""//.*|/\*[^*]*\*/""", "").trim)
                        cache.put(key, new CacheElement(value, Some(expiryInfo.expiry), expiryInfo.setAt))
                    }

                    i += 5
                }
                else if (line == "00" && totalCount > 0) {
                    if (lines.length < (i + 3)) {
                        throw new Exception("Invalid RDB Format")
                    }
                    totalCount -= 1

                    val key = rdbDecoder.string_decoder(lines(i + 1).replaceAll("""//.*|/\*[^*]*\*/""", "").trim)
                    val value = rdbDecoder.string_decoder(lines(i + 2).replaceAll("""//.*|/\*[^*]*\*/""", "").trim)
                    cache.put(key, new CacheElement(value, None, LocalDateTime.now()))

                    i += 3
                } 
                else if (line == "FF") {
                    return
                }
                else {
                    throw new Exception(s"Invalid RDB Format, Unexpected line: ${line}")
                }
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