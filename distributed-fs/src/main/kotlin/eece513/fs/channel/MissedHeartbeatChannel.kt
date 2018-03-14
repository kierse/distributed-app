package eece513.fs.channel

import eece513.fs.Logger
import eece513.fs.mapper.NodeMapper
import eece513.fs.message.ReadableMessageFactory
import eece513.fs.model.Node
import java.nio.channels.ReadableByteChannel

class MissedHeartbeatChannel(
        channel: ReadableByteChannel,
        messageFactory: ReadableMessageFactory,
        nodeMapper: NodeMapper,
        logger: Logger
) : ReadObjectChannel<Node>(RingChannel.Type.PREDECESSOR_MISSED_HEARTBEAT_READ, channel, messageFactory, nodeMapper, logger)
