package eece513

import org.junit.Test

import org.junit.Assert.*
import java.nio.ByteBuffer

class MessageBuilderTest {
    @Test
    fun build() {
        val body = 12345
        val intByteCount = 4 /* num of bytes to hold an int */

        val expected = ByteBuffer.allocate(MESSAGE_HEADER_SIZE + intByteCount)
        expected.clear()
        expected.putShort(intByteCount.toShort()) // header
        expected.putInt(body)                     // body
        expected.flip()

        val buf = ByteBuffer.allocate(intByteCount)
        buf.clear()
        buf.putInt(body)
        buf.flip()

        assertEquals(expected, MessageBuilder().build(buf.array()))
    }
}