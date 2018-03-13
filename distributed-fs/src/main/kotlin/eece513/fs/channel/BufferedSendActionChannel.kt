package eece513.fs.channel

import eece513.common.mapper.ByteMapper
import eece513.fs.message.SendableMessageFactory
import eece513.common.model.Action
import java.nio.channels.WritableByteChannel
import java.util.*

class BufferedSendActionChannel(
        override val type: RingChannel.Type,
        private val channel: WritableByteChannel,
        private val messageFactory: SendableMessageFactory,
        private val actionMapper: ByteMapper<Action>
) : RingChannel {
    private var inProgressSendableMessage: SendableMessageFactory.SendableMessage? = null
    private val pendingActions = LinkedList<Action>()

    fun queue(action: Action) {
        pendingActions.add(action)
    }

    fun send(): Boolean {
        val inProgress = this.inProgressSendableMessage ?: getNextSendableMessage() ?: return false

        inProgress.send(channel)

        this.inProgressSendableMessage = if (inProgress.complete) null else inProgress
        return inProgress.complete
    }

    private fun getNextSendableMessage(): SendableMessageFactory.SendableMessage? {
        val nextAction = pendingActions.pollFirst() ?: return null
        val byteArray = actionMapper.toByteArray(nextAction)
        return messageFactory.create(byteArray)
    }
}
