package eece513.fs.mapper

import eece513.common.mapper.EmptyByteArrayException
import eece513.common.mapper.ParseException
import eece513.fs.Actions
import eece513.fs.model.MembershipList
import eece513.common.model.Node
import org.junit.Test

import org.junit.Assert.*
import java.net.InetSocketAddress
import java.time.Instant

class MembershipListMapperTest {
    private val node = Node(InetSocketAddress("127.0.0.1", 6970), Instant.now())

    @Test
    fun toByteArray() {
        val list = MembershipList(listOf(node))
        val expected = buildByteArray(list)

        val result = MembershipListMapper().toByteArray(list)

        assertArrayEquals(expected, result)
    }

    @Test(expected = EmptyByteArrayException::class)
    fun toMembershipList__empty_byte_list() {
        assertNull(MembershipListMapper().toObject(byteArrayOf()))
    }

    @Test
    fun toMembershipList() {
        val expected = MembershipList(listOf(node))
        val bytes = buildByteArray(expected)

        val result = MembershipListMapper().toObject(bytes)

        assertEquals(expected, result)
    }

    @Test(expected = ParseException::class)
    fun toMembershipList__parse_error() {
        val expected = MembershipList(listOf(node))
        val bytes = buildByteArray(expected)

        val result = MembershipListMapper().toObject(bytes.sliceArray(0..5))

        assertEquals(expected, result)
    }

    private fun buildByteArray(membershipList: MembershipList): ByteArray {
        val builder = Actions.MembershipList.newBuilder()

        for (node in membershipList.nodes) {
            val timestamp = Actions.Timestamp.newBuilder()
                    .setSecondsSinceEpoch(node.joinedAt.epochSecond)
                    .setNanoSeconds(node.joinedAt.nano)
                    .build()
            val membership = Actions.Membership.newBuilder()
                    .setHostName(node.addr.hostName)
                    .setPort(node.addr.port)
                    .setTimestamp(timestamp)
                    .build()
            builder.addNode(membership)
        }

        return builder.build().toByteArray()
    }
}