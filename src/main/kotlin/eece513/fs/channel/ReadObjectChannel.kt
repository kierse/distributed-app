package eece513.fs.channel

import eece513.fs.Logger
import eece513.fs.mapper.ObjectMapper
import eece513.fs.message.ReadableMessageFactory
import java.nio.channels.ReadableByteChannel

open class ReadObjectChannel<out T>(
        override val type: RingChannel.Type,
        private val channel: ReadableByteChannel,
        private val messageFactory: ReadableMessageFactory,
        private val objectMapper: ObjectMapper<T>,
        private val logger: Logger
) : RingChannel {
    private val tag = ReadObjectChannel::class.java.simpleName
    private var inProgressReadableMessage: ReadableMessageFactory.ReadableMessage? = null

    fun read(): T? {
        val inProgress = this.inProgressReadableMessage ?: messageFactory.create()

        inProgress.read(channel)

        this.inProgressReadableMessage = if (inProgress.complete) null else inProgress

        return inProgress.byteArray
                ?.let {
                    try {
                        objectMapper.toObject(it)
                    } catch (e: ObjectMapper.ParseException) {
                        logger.error(tag, "Error mapping heartbeat action: %s", e)
                        null
                    } catch (e: ObjectMapper.EmptyByteArrayException) {
                        logger.error(tag, "Message body empty! %s", e)
                        null
                    }
                }
    }
}