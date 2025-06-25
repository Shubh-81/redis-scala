package codecrafters_redis.utils

class RESPEncoder {

    def encodeBulkString(input: String): String = {
        val len = input.length()
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
}