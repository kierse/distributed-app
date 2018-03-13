package eece513.fs.mapper

import com.google.protobuf.InvalidProtocolBufferException
import eece513.common.mapper.ByteMapper
import eece513.common.mapper.EmptyByteArrayException
import eece513.common.mapper.ObjectMapper
import eece513.fs.Actions
import eece513.fs.model.MembershipList
import eece513.common.model.Node
import java.net.InetSocketAddress
import java.time.Instant

class MembershipListMapper : ObjectMapper<MembershipList>, ByteMapper<MembershipList> {
//    override val type = MembershipList::class.java

    override fun toObjectOrNull(byteArray: ByteArray): MembershipList? {
        if (byteArray.isEmpty()) {
            throw EmptyByteArrayException("byteArray can't be empty!")
        }

        val parsed: Actions.MembershipList
        try {
            parsed = Actions.MembershipList.parseFrom(byteArray)
        } catch (e: InvalidProtocolBufferException) {
            return null
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