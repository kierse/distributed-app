package eece513.fs.mapper

import com.google.protobuf.InvalidProtocolBufferException
import eece513.common.mapper.ByteMapper
import eece513.common.mapper.EmptyByteArrayException
import eece513.common.mapper.ObjectMapper
import eece513.common.model.Action
import eece513.common.model.Node
import eece513.fs.Actions
import java.net.InetSocketAddress
import java.time.Instant

class FileSystemMapper : ObjectMapper<Action.FileSystemAction>, ByteMapper<Action.FileSystemAction> {
    override fun toObjectOrNull(byteArray: ByteArray): Action.FileSystemAction? {
        if (byteArray.isEmpty()) {
            throw EmptyByteArrayException("byteArray can't be empty!")
        }

        val parsed: Actions.FileSystem
        try {
            parsed = Actions.FileSystem.parseFrom(byteArray)
        } catch (e: InvalidProtocolBufferException) {
            return null
        }

        val address = InetSocketAddress(parsed.hostName, parsed.port)
        val instant = Instant.ofEpochSecond(
                parsed.timestamp.secondsSinceEpoch, parsed.timestamp.nanoSeconds.toLong()
        )
        val node = Node(address, instant)

        return Action.FileSystemAction.RemoveFile(parsed.remoteName, node)
    }

    override fun toByteArray(obj: Action.FileSystemAction): ByteArray {
        val time = obj.sender.joinedAt
        val timestamp = Actions.Timestamp.newBuilder()
                .setSecondsSinceEpoch(time.epochSecond)
                .setNanoSeconds(time.nano)
                .build()

        val address = obj.sender.addr

        return Actions.FileSystem.newBuilder()
                .setRemoteName((obj as Action.FileSystemAction.RemoveFile).remoteName)
                .setTimestamp(timestamp)
                .setHostName(address.hostName)
                .setPort(address.port)
                .build()
                .toByteArray()

    }
}