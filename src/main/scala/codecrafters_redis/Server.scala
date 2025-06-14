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

case class Event(outputStream: OutputStream, message: ArrayBuffer[String])

case class CacheElement(value: String, expiry: Option[Long], setAt: LocalDateTime)

case class Config(dir: String, dbFileName: String)

object Server {
    
    final val cache = new ConcurrentHashMap[String, CacheElement]()
    final var config = new Config("", "")

    private def processEvent(event: Event): Unit = {
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
        config = new Config(argMap.get("dir").getOrElse("/temp/redis-files"), argMap.get("dbfilename").getOrElse("dump.rdb"))

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
                        if (line.startsWith("*")) {
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