package eece513.fs.channel

import com.nhaarman.mockito_kotlin.mock
import eece513.fs.DummyLogger
import eece513.fs.mapper.ActionMapper
import eece513.fs.model.Action
import eece513.fs.model.Node
import org.junit.Test

import org.junit.Assert.*
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetSocketAddress
import java.nio.channels.DatagramChannel
import java.time.Instant

class ReadHeartbeatChannelTest {
    private val node = Node(InetSocketAddress("127.0.0.1", 6969), Instant.now())
    private val address = InetSocketAddress("127.0.0.1", 6970)
    private val mapper = ActionMapper()

    @Test
    fun getType() {
        assertEquals(
                RingChannel.Type.PREDECESSOR_HEARTBEAT_READ,
                ReadHeartbeatChannel(mock(), ActionMapper(), DummyLogger()).type
        )
    }

    @Test
    fun read() {
        val heartbeat = Action.Heartbeat(node)
        val message = mapper.toByteArray(heartbeat)

        DatagramChannel.open()
                .bind(address)
                .use { datagramChannel ->
                    val channel = ReadHeartbeatChannel(datagramChannel, mapper, DummyLogger())

                    DatagramSocket().use { socket ->
                        socket.send(DatagramPacket(message, message.size, address))
                    }

                    assertEquals(node, channel.read())
                }
    }

    @Test
    fun read__invalid_action_type() {
        val heartbeat = Action.Join(node)
        val message = buildMessage(mapper.toByteArray(heartbeat))

        DatagramChannel.open()
                .bind(address)
                .use { datagramChannel ->
                    val channel = ReadHeartbeatChannel(datagramChannel, mapper, DummyLogger())

                    DatagramSocket().use { socket ->
                        socket.send(DatagramPacket(message, message.size, address))
                    }

                    assertNull(channel.read())
                }
    }

    @Test
    fun read__parse_error() {
        val byteArray = mapper.toByteArray(Action.Heartbeat(node))

        // shuffle a few bytes so the message is broken
        var temp = byteArray[2]
        byteArray[2] = byteArray.last()
        byteArray[byteArray.size - 1] = temp

        temp = byteArray[3]
        byteArray[3] = byteArray[byteArray.size - 2]
        byteArray[byteArray.size - 2] = temp

        val message = buildMessage(byteArray)

        DatagramChannel.open()
                .bind(address)
                .use { datagramChannel ->
                    val channel = ReadHeartbeatChannel(datagramChannel, mapper, DummyLogger())

                    DatagramSocket().use { socket ->
                        socket.send(DatagramPacket(message, message.size, address))
                    }

                    assertNull(channel.read())
                }
    }
}