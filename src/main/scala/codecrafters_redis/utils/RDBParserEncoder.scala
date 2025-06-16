package codecrafters_redis.utils

import java.time.ZoneOffset
import java.time.LocalDateTime
import java.time.Instant
import java.time.Duration

sealed trait EncodingType 
object EncodingType {
    case object STRING_ENCODING extends EncodingType
    case object SIZE_ENCODING extends EncodingType
}

case class ExpiryInfo(expiry: Long, setAt: LocalDateTime)

class RDBParserEncoder {

    private def string_encoder(input: String): String = {
        val len = input.length()
        var res = size_encoder(len) + " "

        for (ch <- input) {
            res += numToHexa(ch.toLong).reverse
            res += " "
        }

        return res.trim
    }

    def string_decoder(input: String): String = {
        val len = input.length()
        var idx = 0
        
        val bytes = input.split(" ")
        if (bytes.length == 0) {
            throw new Exception("Invalid string to decode")
        }

        val stringLen = size_decoder(bytes(0))
        if (stringLen != (bytes.length - 1)) {
            throw new Exception("String size does not match specified size")
        }

        var res = ""
        for (byte <- bytes) {
            if (byte.length != 2) {
                throw new Exception("Invalid format")
            }
            res += hexaToNum(byte.reverse).toChar
        }

        return res.trim
    }

    def size_decoder(input: String): Long = {
        var bits = ""
        val len = input.length

        var idx = len - 1
        while (idx >= 0) {
            if (input(idx) != ' ') {
                var num = if (input(idx) <= '9' && input(idx) >= '0') input(idx) - '0' else (input(idx) - 'A' + 10)
                var curr = ""
                while (num > 0) {
                    curr += ('0' + num % 2).toChar
                    num /= 2
                }

                while (curr.length < 4) curr += '0'

                bits += curr
            }
            idx -= 1
        }

        val bitLen = bits.length()
        var res = 0

        // In case of starting bits to be 00 take the next six bits as the size
        if (bitLen == 8 && bits(bitLen - 1) == '0' && bits(bitLen - 2) == '0') {
            var idx = 0

            while (idx < math.min(6, bitLen)) {
                res += (1 << idx) * (bits(idx) - '0')
                idx += 1
            }
        }
        else if (bitLen == 16 && bits(bitLen - 1) == '0' && bits(bitLen - 2) == '1') {
            var idx = 0

            while (idx < 14) {
                res += (1 << idx) * (bits(idx) - '0')
                idx += 1
            }
        }
        else if (bitLen == 40 && bits(bitLen - 1) == '1' && bits(bitLen - 2) == '0') {
            var idx = 0

            while (idx < 32) {
                res += (1 << idx) * (bits(idx) - '0')
                idx += 1
            }
        } else {
            throw new Exception("Invalid size encoding")
        }

        return res
    }

    def size_encoder(input: Long): String = {
        try {
            var num = input
            var bits = "";

            while (num > 0) {
                bits += ('0' + num % 2).toChar
                num /= 2
            }

            if (bits.length <= 6) {
                while (bits.length < 8) bits += '0'
            } else if (bits.length <= 14) {
                while (bits.length < 14)    bits += '0'
                bits += "01"
            } else if (bits.length <= 32) {
                while (bits.length < 32)    bits += '0'
                bits += "10000000"
            } else {
                throw new Exception("Excceded size limit")
            }

            var res = ""
            var i = 0
            while (i < bits.length) {
                if (i % 8 == 0) res += " "
                val curr = 1 * (bits(i) - '0') + 2 * (bits(i + 1) - '0') + 4 * (bits(i + 2) - '0') + 8 * (bits(i + 3) - '0')
                if (curr < 10)  res += ('0' + curr).toChar
                else    res += ('A' + (curr - 10)).toChar

                i += 4
            }
            res = res.trim.reverse
            return res
        } catch {
            case e: Exception => println(s"Error while size encoding: ${e.getMessage()}")
            return ""
        }
    }

    // Convert a number to hexa decimal in little edian format
    def numToHexa(input: Long): String = {
        var num = input
        var res = ""

        var space_pos = 2
        while (num > 0) {
            if (res.length == space_pos) {
                res += " "
                space_pos += 3
            }

            var d = num % 16
            num /= 16

            if (d < 10) res += ('0' + d).toChar
            else    res += ('A' + (d - 10)).toChar
        }   

        if (res.length % 2 != 0)    res += "0"
        return res.trim
    }

    // Convert a hexa decimal string (little edian) to number
    def hexaToNum(input: String): Long = {
        var res: Long = 0
        var p = 0

        for (ch <- input) {
            if (ch != ' ') {
                val num = if (ch <= '9' && ch >= '0') ch - '0' else ch - 'A' + 10
                res = res + (math.pow(16, p) * (num)).toLong
                p += 1
            }
        }

        return res
    }

    def expiry_encoder(expiry: Long, setAt: LocalDateTime): String = {
        val millis: Long = setAt.toInstant(ZoneOffset.ofHoursMinutes(5, 30)).toEpochMilli + expiry
        return numToHexa(millis)
    }

    def expiry_decoder(input: String): ExpiryInfo = {
        val num = hexaToNum(input)
        val expiryTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(num), ZoneOffset.ofHoursMinutes(5, 30))
        val expiry = Duration.between(LocalDateTime.now(), expiryTime).toMillis()
        return ExpiryInfo(expiry, LocalDateTime.now())
    }

    def encode(input: String, encodingType: EncodingType): String = {
        if (encodingType == EncodingType.STRING_ENCODING) {
            return string_encoder(input)
        } else if (encodingType == EncodingType.SIZE_ENCODING) {
            try {
                return size_encoder(input.toLong)
            } catch {
                case e: Exception => println(s"Error: ${e.getMessage()}")
                return ""
            } 
        }

        return ""
    }
}