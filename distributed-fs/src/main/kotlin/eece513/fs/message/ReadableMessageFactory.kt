package eece513.fs.message

import eece513.common.MESSAGE_HEADER_SIZE
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel

class ReadableMessageFactory {
    interface ReadableMessage {
        val complete: Boolean
        val byteArray: ByteArray?

        fun read(channel: ReadableByteChannel)
    }

    private class ReadableMessageImpl : ReadableMessage {
        val header: ByteBuffer = ByteBuffer.allocate(MESSAGE_HEADER_SIZE)
        var body: ByteBuffer? = null

        override val complete: Boolean
            get() = !header.hasRemaining() && !(body?.hasRemaining() ?: true)

        override val byteArray: ByteArray?
            get() = if (!complete) null else body?.array()

        override fun read(channel: ReadableByteChannel) {
            if (complete) return

            if (header.hasRemaining()) {
                do {
                    val length = channel.read(header)
                } while (length > 0 && header.hasRemaining())

                // If we get this far and there is still more to be read, it means the last read() attempt
                // returned nothing. Halt execution here and resume the next time this channel is ready to
                // be read from.
                if (header.hasRemaining()) return

                // read short without changing buffer position
                val bodyLength = header.getShort(0)

                // create body buffer
                body = ByteBuffer.allocate(bodyLength.toInt())
            }

            val body = body as ByteBuffer
            do {
                val length = channel.read(body)
            } while (length > 0 && body.hasRemaining())
        }
    }

    fun create(): ReadableMessage = ReadableMessageImpl()
}