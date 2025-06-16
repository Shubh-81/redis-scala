package codecrafters_redis

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import codecrafters_redis.utils.RDBParserEncoder
import codecrafters_redis.utils.EncodingType

class RDBParserEncoderSpec extends AnyFlatSpec with Matchers {
    val rdbParser = new RDBParserEncoder()

    "RDB Parser" should "encode strings correctly" in {
        val input = "redis-ver"
        val result = rdbParser.encode(input, EncodingType.STRING_ENCODING)

        result should be("09 72 65 64 69 73 2D 76 65 72")
    }

    "RDB Parser" should "decode strings correctly" in {
        val input = "09 72 65 64 69 73 2D 76 65 72"
        val result = rdbParser.string_decoder(input)

        result should be("redis-ver")
    }

    "RDB Parser" should "convert nums to hexa properly" in {
        val input = 10
        val result = rdbParser.numToHexa(input)

        result should be("A0")
    }

    "RDB Parser" should "convert hexa to nums properly" in {
        val input = "A0"
        val result = rdbParser.hexaToNum(input)

        result should be(10)
    }

    "RDB Parser" should "encode size correctly" in {
        val input = 10
        val result = rdbParser.size_encoder(input)

        result should be("0A")
    }

    "RDB Parser" should "decode size correctly" in {
        val input = "0A"
        val result = rdbParser.size_decoder(input)

        result should be(10)
    }
} 