package eece513.fs.channel

import eece513.common.mapper.ByteMapper
import eece513.common.model.FsResponse
import eece513.fs.message.SendableMessageFactory
import java.nio.channels.WritableByteChannel

class SendFsResponseChannel(
        type: RingChannel.Type,
        channel: WritableByteChannel,
        messageFactory: SendableMessageFactory,
        mapper: ByteMapper<FsResponse>
) : SendObjectChannel<FsResponse>(type, channel, messageFactory, mapper)