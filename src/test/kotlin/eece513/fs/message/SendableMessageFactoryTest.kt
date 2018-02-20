package eece513.fs.message

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.whenever
import org.junit.Test

import org.junit.Assert.*
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.WritableByteChannel

class SendableMessageFactoryTest {
    private val factory = SendableMessageFactory()
    private val defaultByteArray = byteArrayOf(0.toByte(), 1.toByte(), 2.toByte())

    @Test
    fun read() {
        val message = factory.create(defaultByteArray)
        val stream = ByteArrayOutputStream()

        message.send(Channels.newChannel(stream))

        assertArrayEquals(buildByteArrayMessage(defaultByteArray), stream.toByteArray())
    }

    @Test
    fun read__partial_write() {
        var readListPos = 0
        val reads = arrayOf(3, 0, 2)
        val result = ArrayList<Byte>()

        val channel = mock<WritableByteChannel>()
        whenever(channel.write(any())).thenAnswer { mock ->
            val buffer = mock.getArgument<ByteBuffer>(0)

            var current = 0
            while (current < reads[readListPos]) {
                result.add(buffer.get())

                current++
            }

            reads[readListPos++]
        }

        val message = factory.create(defaultByteArray)

        // first (partial) send
        message.send(channel)

        assertFalse(message.complete)

        // complete send
        message.send(channel)

        assertTrue(message.complete)
        assertArrayEquals(buildByteArrayMessage(defaultByteArray), result.toByteArray())
    }
}