package eece513.fs.channel

import eece513.common.mapper.ByteMapper
import eece513.common.model.ConnectionPurpose
import eece513.fs.message.SendableMessageFactory
import java.nio.channels.WritableByteChannel

class SendConnectionPurposeChannel(
        type: RingChannel.Type,
        channel: WritableByteChannel,
        messageFactory: SendableMessageFactory,
        mapper: ByteMapper<ConnectionPurpose>
) : SendObjectChannel<ConnectionPurpose>(type, channel, messageFactory, mapper)