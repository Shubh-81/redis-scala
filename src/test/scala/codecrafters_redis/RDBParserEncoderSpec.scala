package codecrafters_redis

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import codecrafters_redis.utils.RDBParserEncoder
import codecrafters_redis.utils.EncodingType
import scala.collection.mutable.ArrayBuffer
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.Duration

class RDBParserEncoderSpec extends AnyFlatSpec with Matchers {
    val rdbParser = new RDBParserEncoder()

    "RDB Parser" should "encode strings correctly" in {
        val input = "redis-ver"
        val result = rdbParser.string_encoder(input)

        val hexString = result.map("%02x".format(_)).mkString(" ")
        hexString should be("09 72 65 64 69 73 2d 76 65 72")
    }

    "RDB Parser" should "decode strings correctly" in {
        val input = rdbParser.string_encoder("redis-ver")
        val (result, _) = rdbParser.string_decoder(input, 0)

        result should be("redis-ver")
    }

    "RDB Parser" should "encode expiry correctly" in {
        val input: Long = 100
        val inputTime = LocalDateTime.now()
        val result = rdbParser.expiry_encoder(input, inputTime)

        val (expiry, time, index) = rdbParser.expiry_decoder(result, 0)

        val timeDelta = Duration.between(inputTime, time).toMillis.toLong
        timeDelta should be(input - expiry)
    }

    "RDB Parser" should "encode size correctly" in {
        val input = 10
        val result = rdbParser.size_encoder(input)

        val hexString = result.map(b => f"${b & 0xFF}%02X").mkString(" ")
        hexString should be("0A")
    }

    "RDB Parser" should "decode size correctly" in {
        val input = rdbParser.size_encoder(10)
        val (result, _) = rdbParser.size_decoder(input, 0)

        result should be(10)
    }
} 