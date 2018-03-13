package eece513.fs.channel

import eece513.common.Logger
import eece513.common.mapper.ObjectMapper
import eece513.fs.message.ReadableMessageFactory
import eece513.fs.model.MembershipList
import java.nio.channels.ReadableByteChannel

class ReadMembershipListChannel(
        type: RingChannel.Type,
        channel: ReadableByteChannel,
        messageFactory: ReadableMessageFactory,
        mapper: ObjectMapper<MembershipList>,
        logger: Logger
) : ReadObjectChannel<MembershipList>(type, channel, messageFactory, mapper, logger)
