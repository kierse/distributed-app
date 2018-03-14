package eece513.fs.message

import eece513.fs.MESSAGE_HEADER_SIZE
import java.nio.ByteBuffer

fun buildByteArrayMessage(message: ByteArray): ByteArray {
    val buffer = ByteBuffer.allocate(MESSAGE_HEADER_SIZE + message.size)
    buffer.clear()
    buffer.putShort(message.size.toShort())
    buffer.put(message)
    buffer.flip()
    return buffer.array()
}

