package eece513.fs.channel

import eece513.common.model.FsResponse

class FsResponseStateChannel(
        override val type: RingChannel.Type,
        val response: FsResponse,
        val sendChannel: SendFsResponseChannel
) : RingChannel