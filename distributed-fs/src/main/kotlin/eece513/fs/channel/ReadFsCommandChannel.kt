package eece513.fs.channel

import eece513.common.Logger
import eece513.common.mapper.ObjectMapper
import eece513.common.model.FsCommand
import eece513.fs.message.ReadableMessageFactory
import java.nio.channels.ReadableByteChannel

class ReadFsCommandChannel(
        type: RingChannel.Type,
        channel: ReadableByteChannel,
        messageFactory: ReadableMessageFactory,
        mapper: ObjectMapper<FsCommand>,
        logger: Logger
) : ReadObjectChannel<FsCommand>(type, channel, messageFactory, mapper, logger)