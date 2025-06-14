package codecrafters_redis

import java.net._;
import java.io._;
import scala.util.Using
import scala.collection.mutable.ListBuffer
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.ArrayBuffer
import scala.collection.concurrent.Map
import java.util.concurrent.ConcurrentHashMap

case class Event(outputStream: OutputStream, message: ArrayBuffer[String])

object Server {
    
    final val cache = new ConcurrentHashMap[String, String]()

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

            cache.put(event.message(1), event.message(2))
            event.outputStream.write("+OK\r\n".getBytes())
        } else if (event.message(0).toUpperCase() == "GET") {
            if (event.message.length < 2) {
                throw new Exception("Invalid arguments")
            }

            if (cache.containsKey(event.message(1))) {
                val value = cache.get(event.message(1))
                event.outputStream.write(s"+${value}\r\n".getBytes())
            } else {
                event.outputStream.write("$-1\r\n".getBytes())
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
}