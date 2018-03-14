package eece513.fs.channel

import eece513.fs.mapper.ActionMapper
import eece513.fs.message.SendableMessageFactory
import eece513.fs.model.Action
import java.nio.channels.WritableByteChannel

class SendActionChannel(
        type: RingChannel.Type,
        channel: WritableByteChannel,
        messageFactory: SendableMessageFactory,
        actionMapper: ActionMapper
) : SendObjectChannel<Action>(type, channel, messageFactory, actionMapper)
