package codecrafters_redis

import java.net._;
import java.io._;
import scala.util.Using
import scala.collection.mutable.ListBuffer

object Server {

    def main(args: Array[String]): Unit = {
        val serverSocket = new ServerSocket();
        serverSocket.bind(new InetSocketAddress("localhost", 6379))

        var threads = ListBuffer[Thread]()

        while (true) {
            val clientSocket = serverSocket.accept()

            val thread = new Thread(() => {         
                Using(clientSocket.getInputStream()) { is =>
                    Using(clientSocket.getOutputStream()) { os =>
                        val reader = new BufferedReader(new InputStreamReader(is))

                        reader.lines().forEach { line =>
                            if (line.startsWith("PING")) {
                                os.write("+PONG\r\n".getBytes())
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