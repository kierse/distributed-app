package eece513

import org.junit.Test

import org.junit.Assert.*
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels

class MessageReaderTest {

    @Test
    fun read() {
        val body = 12345
        val intByteCount = 4 /* num of bytes to hold an int */

        val expected = ByteBuffer.allocate(intByteCount)
        expected.clear()
        expected.putInt(body)
        expected.flip()

        val buf = ByteBuffer.allocate(MESSAGE_HEADER_SIZE + intByteCount)
        buf.clear()
        buf.putShort(intByteCount.toShort()) // header
        buf.put(expected)                    // body
        buf.flip()
        expected.flip()

        val byteStream = ByteArrayInputStream(buf.array())

        val byteArray = MessageReader(DummyLogger()).read(Channels.newChannel(byteStream))

        assertArrayEquals(expected.array(), byteArray)
    }

    @Test
    fun read__two_messages() {
        val body1 = 12345
        val body2 = 54321
        val intByteCount = 4 /* num of bytes to hold an int */

        val expected1 = ByteBuffer.allocate(intByteCount)
        expected1.clear()
        expected1.putInt(body1)
        expected1.flip()

        val expected2 = ByteBuffer.allocate(intByteCount)
        expected2.clear()
        expected2.putInt(body2)
        expected2.flip()

        val buf = ByteBuffer.allocate((MESSAGE_HEADER_SIZE + intByteCount) * 2)
        buf.clear()

        // write first message
        buf.putShort(intByteCount.toShort()) // header
        buf.put(expected1)                    // body
        expected1.flip()

        // write second message
        buf.putShort(intByteCount.toShort()) // header
        buf.put(expected2)                    // body
        expected2.flip()

        buf.flip()

        val byteStream = ByteArrayInputStream(buf.array())

        val reader = MessageReader(DummyLogger())
        val channel = Channels.newChannel(byteStream)

        assertArrayEquals(expected1.array(), reader.read(channel))
        assertArrayEquals(expected2.array(), reader.read(channel))
    }

    @Test
    fun read__when_empty() {
        val byteStream = ByteArrayInputStream(ByteArray(0))
        val byteArray = MessageReader(DummyLogger()).read(Channels.newChannel(byteStream))

        assertArrayEquals(ByteArray(0), byteArray)
    }
}