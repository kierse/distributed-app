package eece513.fs.channel

import eece513.common.mapper.ByteMapper
import eece513.fs.message.SendableMessageFactory
import eece513.fs.model.MembershipList
import java.nio.channels.WritableByteChannel

class SendMembershipListChannel(
        type: RingChannel.Type,
        channel: WritableByteChannel,
        messageFactory: SendableMessageFactory,
        mapper: ByteMapper<MembershipList>
) : SendObjectChannel<MembershipList>(type, channel, messageFactory, mapper)
