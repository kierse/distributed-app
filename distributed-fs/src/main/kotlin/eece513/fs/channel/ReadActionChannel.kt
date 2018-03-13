package eece513.fs.channel

import eece513.common.Logger
import eece513.common.mapper.ObjectMapper
import eece513.common.model.Action
import eece513.fs.message.ReadableMessageFactory
import java.nio.channels.ReadableByteChannel

class ReadActionChannel(
        type: RingChannel.Type,
        channel: ReadableByteChannel,
        messageFactory: ReadableMessageFactory,
        mapper: ObjectMapper<Action>,
        logger: Logger
) : ReadObjectChannel<Action>(type, channel, messageFactory, mapper, logger)