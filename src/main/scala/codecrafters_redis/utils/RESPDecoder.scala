package codecrafters_redis.utils;

class RESPDecoder {

    def decodeSimpleString(input: String): String = {
        if (input.length < 3) {
            throw new Exception("Invalid input for simple string")
        }

        return input.substring(1, input.length)
    }
}