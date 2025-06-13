package codecrafters_redis

import java.net._;
import java.io._;
import scala.util.Using

object Server {
  def main(args: Array[String]): Unit = {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println("Logs from your program will appear here!")

    // Uncomment this to pass the first stage
    //
    val serverSocket = new ServerSocket()
    serverSocket.bind(new InetSocketAddress("localhost", 6379))
    val clientSocket = serverSocket.accept();

    Using(clientSocket.getInputStream()) { is =>
      Using (clientSocket.getOutputStream()) { os =>
        var reader = new BufferedReader(new InputStreamReader(is))
        reader.lines().forEach { line => 
          if (line.startsWith("PING")) {
            os.write("+PONG\r\n".getBytes())
          }
        }
      }
    } 
  }
}
