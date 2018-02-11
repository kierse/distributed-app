package eece513

import java.nio.ByteBuffer

class MessageBuilder {
    fun build(bytes: ByteArray): ByteBuffer {
        val messageSize = bytes.size.toShort()

        // Create message buffer
        // Note: add extra bytes for message header
        val buffer = ByteBuffer.allocate(MESSAGE_HEADER_SIZE + messageSize)
        buffer.clear()
        buffer.putShort(messageSize)
        buffer.put(bytes)
        buffer.flip()

        return buffer
    }
}