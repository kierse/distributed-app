package eece513.fs.mapper

import eece513.common.mapper.EmptyByteArrayException
import eece513.common.mapper.ParseException
import eece513.fs.Actions
import eece513.common.model.Node
import org.junit.Test

import org.junit.Assert.*
import java.net.InetSocketAddress
import java.time.Instant

class NodeMapperTest {
    private val node = Node(InetSocketAddress("127.0.0.1", 6969), Instant.now())

    @Test(expected = EmptyByteArrayException::class)
    fun toNode__empty_byte_array() {
        assertNull(NodeMapper().toObject(byteArrayOf()))
    }

    @Test
    fun toNode() {
        val bytes = buildByteArray(node)
        val result = NodeMapper().toObject(bytes)

        assertEquals(node, result)
    }

    @Test(expected = ParseException::class)
    fun toNode__parse_error() {
        val mapper = NodeMapper()
        val bytes = mapper.toByteArray(node)

        mapper.toObject(bytes.sliceArray(0..5))
    }

    @Test
    fun toByteArray() {
        val expected = buildByteArray(node)

        val result = NodeMapper().toByteArray(node)

        assertArrayEquals(expected, result)
    }

    private fun buildByteArray(node: Node): ByteArray {
        val timestamp = Actions.Timestamp.newBuilder()
                .setSecondsSinceEpoch(node.joinedAt.epochSecond)
                .setNanoSeconds(node.joinedAt.nano)
                .build()

        return Actions.Membership.newBuilder()
                .setTimestamp(timestamp)
                .setHostName(node.addr.hostName)
                .setPort(node.addr.port)
                .build()
                .toByteArray()
    }
}