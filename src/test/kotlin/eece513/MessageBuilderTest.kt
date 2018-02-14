package eece513

import org.junit.Test

import org.junit.Assert.*
import java.nio.ByteBuffer

class MessageBuilderTest {
    private val intByteCount = 4 /* num of bytes to hold an int */

    @Test
    fun build() {
        val body = 12345
        assertEquals(buildMessage(body), MessageBuilder().build(intToByteArray(body)))
    }

    @Test
    fun combineMessages() {
        val body1 = 1234
        val body2 = 1234

        val size = MESSAGE_HEADER_SIZE + intByteCount
        val expected = ByteArray(size * 2)

        buildMessage(body1).get(expected, 0, size)
        buildMessage(body2).get(expected, size, size)

        val builder = MessageBuilder()
        val msg1 = builder.build(intToByteArray(body1))
        val msg2 = builder.build(intToByteArray(body2))

        assertEquals(ByteBuffer.wrap(expected), builder.combineMessages(listOf(msg1, msg2)))
    }

    private fun buildMessage(body: Int): ByteBuffer {
        val buffer = ByteBuffer.allocate(MESSAGE_HEADER_SIZE + intByteCount)
        buffer.clear()
        buffer.putShort(intByteCount.toShort()) // header
        buffer.putInt(body)                     // body
        buffer.flip()

        return buffer
    }

    private fun intToByteArray(body: Int): ByteArray {
        val buf = ByteBuffer.allocate(intByteCount)
        buf.clear()
        buf.putInt(body)

        return buf.array()
    }
}