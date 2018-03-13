package eece513.fs.channel

import eece513.common.mapper.ByteMapper
import eece513.common.model.Action
import eece513.fs.message.SendableMessageFactory
import java.nio.channels.WritableByteChannel

class SendClusterActionChannel(
        type: RingChannel.Type,
        channel: WritableByteChannel,
        messageFactory: SendableMessageFactory,
        mapper: ByteMapper<Action.ClusterAction>
) : SendObjectChannel<Action.ClusterAction>(type, channel, messageFactory, mapper)
