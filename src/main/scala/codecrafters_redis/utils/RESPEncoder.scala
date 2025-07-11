package codecrafters_redis.utils

import java.io.File
import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ArrayBuffer

class RESPEncoder {

    def encodeSimpleString(input: String): String = {
        return s"+${input}\r\n"
    }

    def encodeBulkString(input: String): String = {
        val len = input.length()
        if (len == 0) {
            return "$-1\r\n"
        }
        return s"$$${len}\r\n${input}\r\n"
    }

    def encodeArray(input: Array[Any], dataEncoded: Boolean = false): String = {
        val len = input.length

        var res = s"*${len}\r\n"
        for (curr <- input) {

            if (dataEncoded) {
                curr match {
                    case s: String => res += s
                }
            } else {
                curr match {
                    case s: String => res += encodeBulkString(s)
                    case arr: Array[Any] => res += encodeArray(arr)
                }
            }
        }

        return res
    }

    private def bytesToBinary(bytes: Array[Byte]): String = {
        var res = ""
        for (byte <- bytes) {
            res += byte.toBinaryString
        }

        return res
    }

    def encodeRDBFile(file: File): String = {
        val bytes = Files.readAllBytes(file.toPath)

        val binaryString = bytesToBinary(bytes)
        val len = binaryString.length()
        return s"${binaryString}"
    }

    def encodeInteger(input: Int): String = {
        return s":${input}\r\n"
    }

    def encodeSimpleError(error: String): String = {
        return s"-${error}\r\n"
    }

    private def parse_key(key: String): (Long, Int) = {
        val Array(time, idx) = key.split("-")
        (time.toLong, idx.toInt)
    }

    def encodeStream(input: ConcurrentHashMap[String, ConcurrentHashMap[String, String]]): String = {
        var res = s"*${input.size}\r\n"

        val entryIterator = input.entrySet().iterator()
        while (entryIterator.hasNext) {
            val entry = entryIterator.next()

            res += "*2\r\n"
            res += encodeSimpleString(entry.getKey())

            val mapArray = ArrayBuffer[String]()
            
            val subMapIterator = entry.getValue().entrySet().iterator()
            while (subMapIterator.hasNext()) {
                val subEntry = subMapIterator.next()
                mapArray += subEntry.getKey()
                mapArray += subEntry.getValue()
            }

            res += encodeArray(mapArray.toArray)
        }

        return res
    }

    def encodeStreams(input: ConcurrentHashMap[String, ConcurrentHashMap[String, ConcurrentHashMap[String, String]]]): String = {
        var res = s"*${input.size}\r\n"

        if (input.size == 0) {
            return encodeSimpleString("-1")
        }

        var isEmpty = true
        val entryIterator = input.entrySet().iterator()
        while (entryIterator.hasNext) {
            val entry = entryIterator.next()
            if (entry.getValue().size > 0) {
                isEmpty = false
            }

            res += "*2\r\n"
            res += encodeSimpleString(entry.getKey())
            res += encodeStream(entry.getValue())
        }

        if (isEmpty) {
            return encodeBulkString("")
        }
        return res
    }
}