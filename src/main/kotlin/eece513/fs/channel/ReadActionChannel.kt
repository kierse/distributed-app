package eece513.fs.channel

import eece513.fs.Logger
import eece513.fs.mapper.ActionMapper
import eece513.fs.message.ReadableMessageFactory
import eece513.fs.model.Action
import java.nio.channels.ReadableByteChannel

class ReadActionChannel(
        type: RingChannel.Type,
        channel: ReadableByteChannel,
        messageFactory: ReadableMessageFactory,
        actionMapper: ActionMapper,
        logger: Logger
) : ReadObjectChannel<Action>(type, channel, messageFactory, actionMapper, logger)
