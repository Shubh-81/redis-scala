package codecrafters_redis

import java.net._;
import java.io._;
import scala.util.Using
import scala.collection.mutable.ListBuffer
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.ArrayBuffer

case class Event(outputStream: OutputStream, message: String)

object Server {

    private def processEvent(event: Event): Unit = {
        if (event.message.toUpperCase().startsWith("PING")) {
            event.outputStream.write("+PONG\r\n".getBytes())
        } else if (event.message.toUpperCase().startsWith("ECHO")) {
            event.outputStream.write(s"+${event.message.substring(4).trim()}\r\n".getBytes())
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
                                    q.offer(new Event(os, command.mkString(" ")))
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