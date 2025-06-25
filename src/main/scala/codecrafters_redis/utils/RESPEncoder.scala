package codecrafters_redis.utils

class RESPEncoder {

    def encodeSimpleString(input: String): String = {
        val len = input.length()
        return s"$$${len}\r\n${input}\r\n"
    }

    def encodeBulkString(input: Array[String]): String = {
        val len = input.length

        var res = s"*${len}\r\n"
        for (curr <- input) {
            res += encodeSimpleString(curr)
        }

        return res
    }
}