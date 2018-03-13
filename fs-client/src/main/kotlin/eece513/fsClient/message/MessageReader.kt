package eece513.fsClient.message

import eece513.common.Logger
import eece513.common.MESSAGE_HEADER_SIZE
import java.io.InputStream
import java.nio.ByteBuffer

class MessageReader(private val logger: Logger) {
    private val tag = MessageReader::class.java.simpleName

    fun read(stream: InputStream): ByteArray {
        val msgLengthBuffer = ByteArray(MESSAGE_HEADER_SIZE)
        stream.read(msgLengthBuffer)

        val header = ByteBuffer.wrap(msgLengthBuffer).short
        logger.debug(tag, "message body is $header byte(s)")

        val msgBuffer = ByteArray(header.toInt())
        stream.read(msgBuffer)

        return msgBuffer
    }
}