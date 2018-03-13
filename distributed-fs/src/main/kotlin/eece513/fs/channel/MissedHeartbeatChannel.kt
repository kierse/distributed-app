package eece513.fs.channel

import eece513.common.Logger
import eece513.common.mapper.ObjectMapper
import eece513.fs.message.ReadableMessageFactory
import eece513.common.model.Node
import java.nio.channels.ReadableByteChannel

class MissedHeartbeatChannel(
        channel: ReadableByteChannel,
        messageFactory: ReadableMessageFactory,
        mapper: ObjectMapper<Node>,
        logger: Logger
) : ReadObjectChannel<Node>(RingChannel.Type.PREDECESSOR_MISSED_HEARTBEAT_READ, channel, messageFactory, mapper, logger)
