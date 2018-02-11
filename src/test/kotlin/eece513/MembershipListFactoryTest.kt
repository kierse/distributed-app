package eece513

import eece513.model.MembershipList
import eece513.model.Node
import org.junit.Assert.assertEquals
import org.junit.Test
import java.io.ByteArrayInputStream

import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.channels.Channels
import java.time.Instant

class MembershipListFactoryTest {

    @Test
    fun build() {
        val expected = buildMembershipList()
        val transportObject = buildTransportObject(expected)
        val buffer = MessageBuilder().build(transportObject.toByteArray())

        val stream = ByteArrayInputStream(buffer.array())
        val channel = Channels.newChannel(stream)

        val result = MembershipListFactory(MessageReader(DummyLogger())).build(channel)

        assertEquals(expected, result)
    }

    private fun buildMembershipList(): MembershipList {
        val nodes = listOf(
                Node(InetSocketAddress(InetAddress.getByName("127.0.0.1"), 6970), Instant.now()),
                Node(InetSocketAddress(InetAddress.getByName("127.0.0.1"), 6971), Instant.now())
        )
        return MembershipList(nodes)
    }

    private fun buildTransportObject(membershipList: MembershipList): Actions.MembershipList {
        val builder = Actions.MembershipList.newBuilder()

        for (node in membershipList.nodes) {
            val timestamp = Actions.Timestamp.newBuilder()
                    .setSecondsSinceEpoch(node.joinedAt.epochSecond)
                    .setNanoSeconds(node.joinedAt.nano)
                    .build()
            val membership = Actions.Membership.newBuilder()
                    .setHostName(node.addr.hostString)
                    .setPort(node.addr.port)
                    .setTimestamp(timestamp)
                    .build()
            builder.addNode(membership)
        }

        return builder.build()
    }
}