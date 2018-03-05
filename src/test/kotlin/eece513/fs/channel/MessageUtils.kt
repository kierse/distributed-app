package eece513.fs.channel

import eece513.fs.MESSAGE_HEADER_SIZE
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel

fun buildReadableByteChannel(byteArray: ByteArray): ReadableByteChannel {
    val message = buildMessage(byteArray)
    val stream = ByteArrayInputStream(message)
    return Channels.newChannel(stream)
}

fun extractByteArray(byteArray: ByteArray) = byteArray.sliceArray(2.until(byteArray.size))

fun buildMessage(byteArray: ByteArray): ByteArray {
    val buffer = ByteBuffer.allocate(MESSAGE_HEADER_SIZE + byteArray.size)
    buffer.clear()
    buffer.putShort(byteArray.size.toShort())
    buffer.put(byteArray)
    buffer.flip()

    return buffer.array()
}
