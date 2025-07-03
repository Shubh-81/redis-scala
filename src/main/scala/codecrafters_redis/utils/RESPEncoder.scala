package codecrafters_redis.utils

import java.io.File
import java.nio.file.Files

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

    def encodeArray(input: Array[String]): String = {
        val len = input.length

        var res = s"*${len}\r\n"
        for (curr <- input) {
            res += encodeBulkString(curr)
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
}