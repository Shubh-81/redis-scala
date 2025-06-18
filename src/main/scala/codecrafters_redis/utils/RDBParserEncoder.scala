package codecrafters_redis.utils

import java.time.ZoneOffset
import java.time.LocalDateTime
import java.time.Instant
import java.time.Duration
import scala.collection.mutable.ArrayBuffer

sealed trait EncodingType 
object EncodingType {
    case object STRING_ENCODING extends EncodingType
    case object SIZE_ENCODING extends EncodingType
}

case class ExpiryInfo(expiry: Long, setAt: LocalDateTime)   

class RDBParserEncoder {

    def string_encoder(input: String, size_encoding: Boolean = true): Array[Byte] = {
        val len = input.length()
        var res: ArrayBuffer[Byte] = ArrayBuffer[Byte]()

        if (size_encoding) {
            res ++= size_encoder(len)
        }
        for (ch <- input) {
            res += ch.toByte
        }

        return res.toArray
    }

    def string_decoder(input: Array[Byte], index: Int): (String, Int) = {
        if (index >= input.length) {
            println(input.slice(index, input.length).map("%02x".format(_)).mkString("\n"))
            println("End of file unexpectedly");
            return ("", index + 1)
        }

        val (stringLen, newIndex) = size_decoder(input, index)
        var idx = 0
        var res = ""
        while (idx < stringLen) {
            if ((newIndex + idx) >= input.length) {
                println(input.slice(index, input.length).map("%02x".format(_)).mkString("\n"))
                println("Unexpected EOF: string")
                return ("", index + 1)
            }
            res += input(newIndex + idx).toChar
            idx += 1
        }

        (res, newIndex + idx)
    }

    def size_decoder(input: Array[Byte], index: Int): (Long, Int) = {
        if (index >= input.length) {
            println(input.slice(index, input.length).map("%02x".format(_)).mkString("\n"))
            println("End of file unexpectedly");
            return (0, index + 1)
        }

        if ((input(index).toLong & 0xC0) == 0x00) {
            (input(index).toLong, index + 1)
        } else if ((input(index).toLong & 0xC0) == 0x40) {
            if (index == input.length - 1) {
                println(input.slice(index, input.length).map("%02x".format(_)).mkString("\n"))
                println("Unexpected end of file")
                return (0, index + 1)
            }
            var res = (input(index).toLong & 0x3F).toLong << 8
            res += (input(index + 1).toLong & 0xFF).toLong

            (res, index + 2)
        } else if ((input(index).toLong & 0xC0) == 0x80) {
            if ((index + 5) > input.length) {
                println(input.slice(index, input.length).map("%02x".format(_)).mkString("\n"))
                println("Unexpected end of file")
                return (0, index + 1)
            }
            var res = (input(index + 1).toLong & 0xFF).toLong << 24
            res += (input(index + 2).toLong & 0xFF).toLong << 16
            res += (input(index + 3).toLong & 0xFF).toLong << 8
            res += (input(index + 4).toLong & 0xFF).toLong

            (res, index + 5)
        } else {
            println(input.slice(index, input.length).map("%02x".format(_)).mkString(s" ${index} \n"))
            (0, index + 1)
        }
    }

    def size_encoder(input: Long): Array[Byte] = {
        if (input < 0) {
            println("Size cannot be less than zero")
        }

        if (input < (1 << 6)) {
            Array((input & 0x3F).toByte)
        } else if (input < (1 << 14)) {
            val b0 = ((input >> 8) & 0x3F | 0x40).toByte
            val b1 = (input & 0xFF).toByte
            Array(b0, b1)
        } else if (input < (1 << 32)) {
            val b0 = 0x80.toByte
            val b1 = ((input >> 24) & 0xFF).toByte
            val b2 = ((input >> 16) & 0xFF).toByte
            val b3 = ((input >> 8) & 0xFF).toByte
            val b4 = (input & 0xFF).toByte
            Array(b0, b1, b2, b3, b4)
        } else {
            println("Size exceeds limit of 32 bits")
            return Array()
        }
    }

    def expiry_encoder(expiry: Long, setAt: LocalDateTime): Array[Byte] = {
        val millis: Long = setAt.toInstant(ZoneOffset.ofHoursMinutes(5, 30)).toEpochMilli + expiry
        var bytes: ArrayBuffer[Byte] = ArrayBuffer[Byte]()

        var i = 0
        while (i < 8) {
            bytes += ((millis >> (i * 8)) & 0xFF).toByte
            i += 1
        }
        return bytes.toArray
    }

    def expiry_decoder(input: Array[Byte], index: Int): (Long, LocalDateTime, Int) = {
        if ((index + 8) > input.length) {
            println(input.slice(index, input.length).map("%02x".format(_)).mkString("\n"))
            println("Unexpected EOF: Expiry")
            return (0, LocalDateTime.now(), index + 1)
        }

        var millis = 0L
        var i = 0
        while (i < 8) {
            millis += ((input(index + i).toLong & 0xFF).toLong << (i * 8)).toLong
            i += 1
        }

        val expiryTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.ofHoursMinutes(5, 30))
        val expiry = Duration.between(LocalDateTime.now(), expiryTime).toMillis()
        return (expiry, LocalDateTime.now(), index + i)
    }

    def encode(input: String, encodingType: EncodingType): String = {
        return ""
    }
}