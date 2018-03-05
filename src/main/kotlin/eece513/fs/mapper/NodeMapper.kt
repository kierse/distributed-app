package eece513.fs.mapper

import com.google.protobuf.InvalidProtocolBufferException
import eece513.fs.Actions
import eece513.fs.model.Node
import java.net.InetSocketAddress
import java.time.Instant

class NodeMapper : ObjectMapper<Node> {
    override fun toObject(byteArray: ByteArray): Node {
        if (byteArray.isEmpty()) {
            throw ObjectMapper.EmptyByteArrayException("byteArray can't be empty!")
        }

        val parsed: Actions.Membership?
        try {
            parsed = Actions.Membership.parseFrom(byteArray)
        } catch (e: InvalidProtocolBufferException) {
            throw ObjectMapper.ParseException(e)
        }

        val address = InetSocketAddress(parsed.hostName, parsed.port)
        val instant = Instant.ofEpochSecond(
                parsed.timestamp.secondsSinceEpoch, parsed.timestamp.nanoSeconds.toLong()
        )

        return Node(address, instant)
    }

    override fun toByteArray(obj: Node): ByteArray {
        val timestamp = Actions.Timestamp.newBuilder()
                .setSecondsSinceEpoch(obj.joinedAt.epochSecond)
                .setNanoSeconds(obj.joinedAt.nano)
                .build()

        return Actions.Membership.newBuilder()
                .setTimestamp(timestamp)
                .setHostName(obj.addr.hostName)
                .setPort(obj.addr.port)
                .build()
                .toByteArray()
    }
}