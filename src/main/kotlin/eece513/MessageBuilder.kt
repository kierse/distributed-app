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

    fun buildDatagram(bytes: ByteArray): ByteBuffer {
        val buffer = ByteBuffer.allocate(bytes.size)
        buffer.clear()
        buffer.put(bytes)
        buffer.flip()

        return buffer
    }

    fun combineMessages(buffers: List<ByteBuffer>): ByteBuffer {
        val byteList = mutableListOf<Byte>()
        buffers.forEach { buffer ->
            byteList.addAll(buffer.array().asList())
        }

        return ByteBuffer.wrap(byteList.toByteArray())
    }
}