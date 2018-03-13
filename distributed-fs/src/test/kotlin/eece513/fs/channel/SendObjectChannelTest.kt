package eece513.fs.channel

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.whenever
import eece513.fs.mapper.ClusterActionMapper
import eece513.fs.message.SendableMessageFactory
import eece513.common.model.Node
import eece513.common.model.Action
import org.junit.Test

import org.junit.Assert.*
import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.nio.channels.Channels
import java.time.Instant

class SendObjectChannelTest {
    private val node = Node(InetSocketAddress("127.0.0.1", 6969), Instant.now())
    private val factory = SendableMessageFactory()
    private val mapper = ClusterActionMapper()

    @Test
    fun send() {
        val action = Action.ClusterAction.Join(node)
        val stream = ByteArrayOutputStream()
        val channel = SendObjectChannel(RingChannel.Type.NODE_ACCEPT, Channels.newChannel(stream), factory, mapper)

        channel.send(action)

        assertEquals(action, mapper.toObject(extractByteArray(stream.toByteArray())))
    }

    @Test
    fun send__partial_send() {
        val action = Action.ClusterAction.Join(node)
        val byteArray = buildMessage(mapper.toByteArray(action))

        val factory = mock<SendableMessageFactory>()
        whenever(factory.create(any())).thenReturn(TestWritableMessage(byteArray, 5, byteArray.size - 5))

        val stream = ByteArrayOutputStream()
        val channel = SendObjectChannel(RingChannel.Type.NODE_ACCEPT, Channels.newChannel(stream), factory, mapper)

        assertFalse(channel.send(action))
        assertTrue(channel.send(action))

        assertEquals(action, mapper.toObject(extractByteArray(stream.toByteArray())))
    }

    @Test
    fun getType() {
        assertEquals(
                RingChannel.Type.NODE_ACCEPT,
                SendObjectChannel(RingChannel.Type.NODE_ACCEPT, mock(), factory, mapper).type
        )
    }
}