package eece513.fs.message

import eece513.fs.MESSAGE_HEADER_SIZE
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel

class SendableMessageFactory {
    interface SendableMessage {
        val complete: Boolean

        fun send(channel: WritableByteChannel)
    }

    private class SendableMessageImpl(byteArray: ByteArray) : SendableMessage {
        val buffer: ByteBuffer = ByteBuffer.allocate(MESSAGE_HEADER_SIZE + byteArray.size)

        init {
            buffer.clear()
            buffer.putShort(byteArray.size.toShort())
            buffer.put(byteArray)
            buffer.flip()
        }

        override val complete: Boolean
            get() = !buffer.hasRemaining()

        override fun send(channel: WritableByteChannel) {
            if (complete) return

            do {
                val length = channel.write(buffer)
            } while (length > 0 && buffer.hasRemaining())
        }
    }

    fun create(byteArray: ByteArray): SendableMessage = SendableMessageImpl(byteArray)
}