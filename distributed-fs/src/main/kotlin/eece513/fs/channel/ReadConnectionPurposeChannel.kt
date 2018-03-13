package eece513.fs.channel

import eece513.common.Logger
import eece513.common.mapper.ObjectMapper
import eece513.fs.message.ReadableMessageFactory
import eece513.common.model.ConnectionPurpose
import java.nio.channels.ReadableByteChannel

class ReadConnectionPurposeChannel(
        type: RingChannel.Type,
        channel: ReadableByteChannel,
        messageFactory: ReadableMessageFactory,
        mapper: ObjectMapper<ConnectionPurpose>,
        logger: Logger
) : ReadObjectChannel<ConnectionPurpose>(type, channel, messageFactory, mapper, logger)
