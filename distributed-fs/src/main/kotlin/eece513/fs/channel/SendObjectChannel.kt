package eece513.fs.channel

import eece513.common.mapper.ByteMapper
import eece513.fs.message.SendableMessageFactory
import java.nio.channels.WritableByteChannel

open class SendObjectChannel<in T>(
        override val type: RingChannel.Type,
        private val channel: WritableByteChannel,
        private val messageFactory: SendableMessageFactory,
        private val objectMapper: ByteMapper<T>
) : RingChannel {
    private var inProgressSendableMessage: SendableMessageFactory.SendableMessage? = null

    fun send(obj: T): Boolean {
        val inProgress = this.inProgressSendableMessage ?: getSendableMessage(obj)

        inProgress.send(channel)

        this.inProgressSendableMessage = if (inProgress.complete) null else inProgress
        return inProgress.complete
    }

    private fun getSendableMessage(obj: T): SendableMessageFactory.SendableMessage {
        val byteArray = objectMapper.toByteArray(obj)
        return messageFactory.create(byteArray)
    }
}
