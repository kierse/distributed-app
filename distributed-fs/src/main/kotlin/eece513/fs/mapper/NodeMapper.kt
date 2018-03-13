package eece513.fs.mapper

import com.google.protobuf.InvalidProtocolBufferException
import eece513.common.mapper.ByteMapper
import eece513.common.mapper.EmptyByteArrayException
import eece513.common.mapper.ObjectMapper
import eece513.fs.Actions
import eece513.common.model.Node
import java.net.InetSocketAddress
import java.time.Instant

class NodeMapper : ObjectMapper<Node>, ByteMapper<Node> {
//    override val type = Node::class.java

    override fun toObjectOrNull(byteArray: ByteArray): Node? {
        if (byteArray.isEmpty()) {
            throw EmptyByteArrayException("byteArray can't be empty!")
        }

        val parsed: Actions.Membership?
        try {
            parsed = Actions.Membership.parseFrom(byteArray)
        } catch (e: InvalidProtocolBufferException) {
            return null
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