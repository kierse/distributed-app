package eece513.fs.mapper

import com.google.protobuf.InvalidProtocolBufferException
import eece513.fs.Actions
import eece513.fs.model.MembershipList
import eece513.fs.model.Node
import java.net.InetSocketAddress
import java.time.Instant

class MembershipListMapper : ObjectMapper<MembershipList> {
    override fun toObject(byteArray: ByteArray): MembershipList {
        if (byteArray.isEmpty()) {
            throw ObjectMapper.EmptyByteArrayException("byteArray can't be empty!")
        }

        val parsed: Actions.MembershipList
        try {
            parsed = Actions.MembershipList.parseFrom(byteArray)
        } catch (e: InvalidProtocolBufferException) {
            throw ObjectMapper.ParseException(e)
        }

        val nodes = mutableListOf<Node>()
        for (membership in parsed.nodeList) {
            val address = InetSocketAddress(membership.hostName, membership.port)
            val timestamp = Instant.ofEpochSecond(
                    membership.timestamp.secondsSinceEpoch,
                    membership.timestamp.nanoSeconds.toLong()
            )
            nodes.add(Node(address, timestamp))
        }

        return MembershipList(nodes)
    }

    override fun toByteArray(obj: MembershipList): ByteArray {
        val builder = Actions.MembershipList.newBuilder()

        obj.nodes.forEach { node ->
            val timestamp = Actions.Timestamp.newBuilder()
                    .setSecondsSinceEpoch(node.joinedAt.epochSecond)
                    .setNanoSeconds(node.joinedAt.nano)
                    .build()

            val member = Actions.Membership.newBuilder()
                    .setHostName(node.addr.hostName)
                    .setPort(node.addr.port)
                    .setTimestamp(timestamp)
                    .build()
            builder.addNode(member)
        }

        return builder.build().toByteArray()
    }
}