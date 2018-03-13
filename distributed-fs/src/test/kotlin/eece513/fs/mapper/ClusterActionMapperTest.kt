package eece513.fs.mapper

import eece513.common.mapper.EmptyByteArrayException
import eece513.fs.Actions
import eece513.common.model.Node
import eece513.common.model.Action
import org.junit.Test

import org.junit.Assert.*
import java.net.InetSocketAddress
import java.time.Instant

class ClusterActionMapperTest {
    private val node = Node(InetSocketAddress("127.0.0.1", 6969), Instant.now())
    private val mapper = ClusterActionMapper()

    @Test(expected = EmptyByteArrayException::class)
    fun toObjectOrNull__empty_byte_array() {
        mapper.toObjectOrNull(byteArrayOf())
    }

    @Test
    fun toObjectOrNull__join() {
        val expected = Action.ClusterAction.Join(node)
        val bytes = buildByteArray(expected)

        val result = mapper.toObjectOrNull(bytes)

        assertEquals(expected, result)
    }

    @Test
    fun toObjectOrNull__remove() {
        val expected = Action.ClusterAction.Leave(node)
        val bytes = buildByteArray(expected)

        val result = mapper.toObjectOrNull(bytes)

        assertEquals(expected, result)
    }

    @Test
    fun toObjectOrNull__drop() {
        val expected = Action.ClusterAction.Drop(node)
        val bytes = buildByteArray(expected)

        val result = mapper.toObjectOrNull(bytes)

        assertEquals(expected, result)
    }

    @Test
    fun toObjectOrNull__connect() {
        val expected = Action.ClusterAction.Connect(node)
        val bytes = buildByteArray(expected)

        val result = mapper.toObjectOrNull(bytes)

        assertEquals(expected, result)
    }

    @Test
    fun toObjectOrNull__heartbeat() {
        val expected = Action.ClusterAction.Heartbeat(node)
        val bytes = buildByteArray(expected)

        val result = mapper.toObjectOrNull(bytes)

        assertEquals(expected, result)
    }

    @Test
    fun toObjectOrNull__parse_error() {
        val expected = Action.ClusterAction.Heartbeat(node)
        val bytes = buildByteArray(expected)

        assertNull(mapper.toObjectOrNull(bytes.sliceArray(0..5)))
    }

    @Test
    fun toByteArray__join() {
        val action = Action.ClusterAction.Join(node)
        val expected = buildByteArray(action)

        val result = mapper.toByteArray(action)

        assertArrayEquals(expected, result)
    }

    @Test
    fun toByteArray__leave() {
        val action = Action.ClusterAction.Leave(node)
        val expected = buildByteArray(action)

        val result = mapper.toByteArray(action)

        assertArrayEquals(expected, result)
    }

    @Test
    fun toByteArray__drop() {
        val action = Action.ClusterAction.Drop(node)
        val expected = buildByteArray(action)

        val result = mapper.toByteArray(action)

        assertArrayEquals(expected, result)
    }

    @Test
    fun toByteArray__connect() {
        val action = Action.ClusterAction.Connect(node)
        val expected = buildByteArray(action)

        val result = mapper.toByteArray(action)

        assertArrayEquals(expected, result)
    }

    @Test
    fun toByteArray__heartbeat() {
        val action = Action.ClusterAction.Heartbeat(node)
        val expected = buildByteArray(action)

        val result = mapper.toByteArray(action)

        assertArrayEquals(expected, result)
    }

    private fun buildByteArray(action: Action.ClusterAction): ByteArray {
        val type = when (action) {
            is Action.ClusterAction.Join -> Actions.Request.Type.JOIN
            is Action.ClusterAction.Leave -> Actions.Request.Type.REMOVE
            is Action.ClusterAction.Drop -> Actions.Request.Type.DROP
            is Action.ClusterAction.Connect -> Actions.Request.Type.CONNECT
            is Action.ClusterAction.Heartbeat -> Actions.Request.Type.HEARTBEAT
        }

        val now = action.node.joinedAt
        val timestamp = Actions.Timestamp.newBuilder()
                .setSecondsSinceEpoch(now.epochSecond)
                .setNanoSeconds(now.nano)
                .build()

        val address = action.node.addr
        return Actions.Request.newBuilder()
                .setTimestamp(timestamp)
                .setType(type)
                .setHostName(address.hostName)
                .setPort(address.port)
                .build()
                .toByteArray()
    }
}