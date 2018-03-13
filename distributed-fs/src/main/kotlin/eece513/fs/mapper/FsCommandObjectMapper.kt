package eece513.fs.mapper

import com.google.protobuf.InvalidProtocolBufferException
import eece513.common.Fs
import eece513.common.mapper.EmptyByteArrayException
import eece513.common.mapper.ObjectMapper
import eece513.common.model.FsCommand

class FsCommandObjectMapper : ObjectMapper<FsCommand> {
    override fun toObjectOrNull(byteArray: ByteArray): FsCommand? {
        if (byteArray.isEmpty()) {
            throw EmptyByteArrayException("byteArray can't be empty!")
        }

        val parsed: Fs.Command
        try {
            parsed = Fs.Command.parseFrom(byteArray)
        } catch (e: InvalidProtocolBufferException) {
            return null
        }

        return when (parsed.type) {
            Fs.Command.Type.CONFIRM -> FsCommand.PutConfirm(parsed.body)
            Fs.Command.Type.GET -> FsCommand.FsGet(parsed.body, "")
            Fs.Command.Type.PUT -> FsCommand.FsPut("", parsed.body)
            Fs.Command.Type.REMOVE -> FsCommand.FsRemove(parsed.body)

            else -> throw IllegalArgumentException("unrecognized type! ${parsed.type.name}")
        }
    }
}