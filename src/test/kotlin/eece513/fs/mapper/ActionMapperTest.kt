package eece513.fs.mapper

import eece513.fs.Actions
import eece513.fs.model.Action
import eece513.fs.model.Node
import org.junit.Test

import org.junit.Assert.*
import java.net.InetSocketAddress
import java.time.Instant

class ActionMapperTest {
    private val node = Node(InetSocketAddress("127.0.0.1", 6969), Instant.now())

    @Test(expected = ObjectMapper.EmptyByteArrayException::class)
    fun toAction__empty_byte_array() {
        ActionMapper().toObject(byteArrayOf())
    }

    @Test
    fun toAction__join() {
        val expected = Action.Join(node)
        val bytes = buildByteArray(expected)

        val result = ActionMapper().toObject(bytes)

        assertEquals(expected, result)
    }

    @Test
    fun toAction__remove() {
        val expected = Action.Leave(node)
        val bytes = buildByteArray(expected)

        val result = ActionMapper().toObject(bytes)

        assertEquals(expected, result)
    }

    @Test
    fun toAction__drop() {
        val expected = Action.Drop(node)
        val bytes = buildByteArray(expected)

        val result = ActionMapper().toObject(bytes)

        assertEquals(expected, result)
    }

    @Test
    fun toAction__connect() {
        val expected = Action.Connect(node)
        val bytes = buildByteArray(expected)

        val result = ActionMapper().toObject(bytes)

        assertEquals(expected, result)
    }

    @Test
    fun toAction__heartbeat() {
        val expected = Action.Heartbeat(node)
        val bytes = buildByteArray(expected)

        val result = ActionMapper().toObject(bytes)

        assertEquals(expected, result)
    }

    @Test(expected = ObjectMapper.ParseException::class)
    fun toAction__parse_error() {
        val expected = Action.Heartbeat(node)
        val bytes = buildByteArray(expected)

        ActionMapper().toObject(bytes.sliceArray(0..5))
    }

    @Test
    fun toByteArray__join() {
        val action = Action.Join(node)
        val expected = buildByteArray(action)

        val result = ActionMapper().toByteArray(action)

        assertArrayEquals(expected, result)
    }

    @Test
    fun toByteArray__leave() {
        val action = Action.Leave(node)
        val expected = buildByteArray(action)

        val result = ActionMapper().toByteArray(action)

        assertArrayEquals(expected, result)
    }

    @Test
    fun toByteArray__drop() {
        val action = Action.Drop(node)
        val expected = buildByteArray(action)

        val result = ActionMapper().toByteArray(action)

        assertArrayEquals(expected, result)
    }

    @Test
    fun toByteArray__connect() {
        val action = Action.Connect(node)
        val expected = buildByteArray(action)

        val result = ActionMapper().toByteArray(action)

        assertArrayEquals(expected, result)
    }

    @Test
    fun toByteArray__heartbeat() {
        val action = Action.Heartbeat(node)
        val expected = buildByteArray(action)

        val result = ActionMapper().toByteArray(action)

        assertArrayEquals(expected, result)
    }

    private fun buildByteArray(action: Action): ByteArray {
        val type = when (action) {
            is Action.Join -> Actions.Request.Type.JOIN
            is Action.Leave -> Actions.Request.Type.REMOVE
            is Action.Drop -> Actions.Request.Type.DROP
            is Action.Connect -> Actions.Request.Type.CONNECT
            is Action.Heartbeat -> Actions.Request.Type.HEARTBEAT
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