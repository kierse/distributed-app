package eece513

import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel

class MessageReader(private val logger: Logger) {
    private val tag = MessageReader::class.java.simpleName

    fun read(channel: ReadableByteChannel): ByteArray {
        val msgLengthBuffer = ByteBuffer.allocate(MESSAGE_HEADER_SIZE)
        msgLengthBuffer.clear()

        while (msgLengthBuffer.hasRemaining()) {
            val read = channel.read(msgLengthBuffer)
            if (read <= 0) break
        }

        if (msgLengthBuffer.position() == 0) return ByteArray(0)

        logger.debug(tag, "read ${msgLengthBuffer.position()} header byte(s)")

        msgLengthBuffer.flip()
        val header = msgLengthBuffer.short.toInt()

        logger.debug(tag, "message body is $header byte(s)")

        val msgBuffer = ByteBuffer.allocate(header)
        while (msgBuffer.hasRemaining()) {
            val read = channel.read(msgBuffer)
            if (read <= 0) break
        }

        logger.debug(tag, "read ${msgBuffer.position()} message body byte(s)")
        return msgBuffer.flip().array()
    }
}