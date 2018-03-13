package eece513.fsClient.mapper

import eece513.common.Fs
import eece513.common.mapper.ByteMapper
import eece513.common.model.FsCommand

class FsCommandByteMapper : ByteMapper<FsCommand> {
    override fun toByteArray(obj: FsCommand): ByteArray {
        val (type, body) = when (obj) {
            is FsCommand.FsRemove -> {
                Pair(Fs.Command.Type.REMOVE, obj.remoteFile)
            }

            is FsCommand.FsGet -> {
                Pair(Fs.Command.Type.GET, obj.remoteFile)
            }

            is FsCommand.FsPut -> {
                Pair(Fs.Command.Type.PUT, obj.remoteFile)
            }

            is FsCommand.PutConfirm -> {
                Pair(Fs.Command.Type.CONFIRM, obj.pathToFile)
            }
        }

        return Fs.Command.newBuilder()
                .setType(type)
                .setBody(body)
                .build()
                .toByteArray()
    }
}