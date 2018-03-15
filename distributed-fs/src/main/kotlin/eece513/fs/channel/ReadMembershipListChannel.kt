package eece513.fs.channel

import eece513.fs.Logger
import eece513.fs.mapper.MembershipListMapper
import eece513.fs.message.ReadableMessageFactory
import eece513.fs.model.MembershipList
import java.nio.channels.ReadableByteChannel

class ReadMembershipListChannel(
        type: RingChannel.Type,
        channel: ReadableByteChannel,
        messageFactory: ReadableMessageFactory,
        membershipListMapper: MembershipListMapper,
        logger: Logger
) : ReadObjectChannel<MembershipList>(type, channel, messageFactory, membershipListMapper, logger)
