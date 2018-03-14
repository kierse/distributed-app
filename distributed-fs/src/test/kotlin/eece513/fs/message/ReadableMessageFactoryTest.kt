package eece513.fs.message

import org.junit.Test

import org.junit.Assert.*
import java.io.ByteArrayInputStream
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel

class ReadableMessageFactoryTest {
    private val factory = ReadableMessageFactory()
    private val defaultByteArray = byteArrayOf(0.toByte(), 1.toByte(), 2.toByte())

    @Test
    fun read() {
        val array = buildByteArrayMessage(defaultByteArray)
        val message = factory.create()

        message.read(buildChannel(array))

        assertArrayEquals(defaultByteArray, message.byteArray)
    }

    @Test
    fun read__partial_header() {
        val array = buildByteArrayMessage(defaultByteArray)
        val message = factory.create()

        message.read(buildChannel(array.sliceArray(0..0)))

        assertFalse(message.complete)
        assertNull(message.byteArray)

        message.read(buildChannel(array.sliceArray(1..(array.size - 1))))

        assertTrue(message.complete)
        assertArrayEquals(defaultByteArray, message.byteArray)
    }

    @Test
    fun read__partial_body() {
        val array = buildByteArrayMessage(defaultByteArray)
        val message = factory.create()

        message.read(buildChannel(array.sliceArray(0..2)))

        assertFalse(message.complete)
        assertNull(message.byteArray)

        message.read(buildChannel(array.sliceArray(3..(array.size - 1))))

        assertTrue(message.complete)
        assertArrayEquals(defaultByteArray, message.byteArray)
    }

    private fun buildChannel(byteArray: ByteArray): ReadableByteChannel {
        val stream = ByteArrayInputStream(byteArray)
        return Channels.newChannel(stream)
    }
}