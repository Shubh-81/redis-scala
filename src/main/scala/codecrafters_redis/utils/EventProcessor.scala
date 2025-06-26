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

class EventProcessor(
    val outputStream: OutputStream,
    val cache: ConcurrentHashMap[String, CacheElement],
    val config: Config
) {

    final val respEncoder = new RESPEncoder()

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

    def process_event(event: Array[String]): Unit = {
        if (event.length == 0) {
            throw new Exception("Empty event")
        }

        val command = event(0).toUpperCase()
        command match {
            case "PING" => process_ping()
            case "ECHO" => process_echo(event)
            case "SET" => process_set(event)
            case "GET" => process_get(event)
            case "CONFIG" => process_config(event)
            case "SAVE" => process_save()
            case "KEYS" => process_keys(event)
            case "INFO" => process_info()
            case "REPLCONF" => process_replconf(event)
            case "PSYNC" => process_psync(event)
            case _ => throw new Exception("Unsupported command")
        }
    }

    private def process_ping(): Unit = {
        outputStream.write(respEncoder.encodeSimpleString("PONG").getBytes())
    }

    private def process_echo(event: Array[String]): Unit = {
        if (event.length != 2) {
            throw new Exception("Invalid Inputs, required: ECHO <command>")
        }

        outputStream.write(respEncoder.encodeSimpleString(event(1)).getBytes())
    }

    private def process_set(event: Array[String]): Unit = {
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
        cache.put(key, new CacheElement(value, exp, LocalDateTime.now()))

        outputStream.write(respEncoder.encodeSimpleString("OK").getBytes())
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
                        outputStream.write(respEncoder.encodeSimpleString(value.value).getBytes())
                    } else {
                        // Remove if expired
                        cache.remove(key)
                        outputStream.write(respEncoder.encodeBulkString("").getBytes())
                    }
                }
                case None => outputStream.write(respEncoder.encodeSimpleString(value.value).getBytes())
            }
        } else {
            outputStream.write(respEncoder.encodeBulkString("").getBytes())
        }
    }

    private def process_config(event: Array[String]): Unit = {
        if (event.length != 3 || event(1).toUpperCase() != "GET") {
            throw new Exception("Invalid Inputs, required: CONFIG GET <key>")
        }

        event(2) match {
            case "dir" => outputStream.write(respEncoder.encodeArray(Array("dir", config.dir)).getBytes())
            case "dbfilename" => outputStream.write(respEncoder.encodeArray(Array("dbfilename", config.dbFileName)).getBytes())
            case _ => throw new Exception("Invalid Inputs, required: key = dir/dbfilename")
        }
    }

    private def process_save(): Unit = {
        // Save current cache state to RDB File
        saveState()
        outputStream.write(respEncoder.encodeSimpleString("OK").getBytes())
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
        outputStream.write(respEncoder.encodeArray(filteredKeys.toArray).getBytes())
    }

    private def process_info(): Unit = {
        outputStream.write(
            respEncoder.encodeBulkString(
                s"role:${config.role}\n" +
                s"master_replid:${config.master_replid}\n" +
                s"master_repl_offset:${config.master_repl_offset}"
            ).getBytes())
    }

    private def process_replconf(event: Array[String]): Unit = {
        if (event.length != 3) {
            throw new Exception("Invalid Inputs, required: REPLCONF <arg1> <arg2>")
        }

        // Return OK for now
        outputStream.write(respEncoder.encodeSimpleString("OK").getBytes())
    }

    private def process_psync(event: Array[String]): Unit = {
        if (event.length != 3) {
            throw new Exception("Invalid Inputs, required: PSYNC ? -1")
        }

        outputStream.write(respEncoder.encodeSimpleString(s"FULLRESYNC ${config.master_replid} ${config.master_repl_offset}").getBytes())
        saveState()
        
        val dir = new File(config.dir)
        val file = new File(dir, config.dbFileName)
        val bytes = Files.readAllBytes(file.toPath)
        outputStream.write(s"$$${file.length()}\r\n".getBytes())
        outputStream.write(bytes)
    }
}