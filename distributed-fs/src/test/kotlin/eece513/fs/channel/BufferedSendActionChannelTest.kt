package eece513.fs.channel

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.whenever
import eece513.common.MESSAGE_HEADER_SIZE
import eece513.fs.mapper.ClusterActionMapper
import eece513.fs.message.SendableMessageFactory
import eece513.common.model.Action
import eece513.common.model.Node
import eece513.fs.mapper.ActionMapper
import eece513.fs.mapper.FileSystemMapper
import org.junit.Test

import org.junit.Assert.*
import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.time.Instant

class BufferedSendActionChannelTest {
    private val node = Node(InetSocketAddress("127.0.0.1", 6969), Instant.now())
    private val factory = SendableMessageFactory()
    private val mapper = ActionMapper(ClusterActionMapper(), FileSystemMapper())

    @Test
    fun sendAction__partial_send() {
        val action = Action.ClusterAction.Join(node)
        val byteArray = buildMessage(mapper.toByteArray(action))

        val factory = mock<SendableMessageFactory>()
        whenever(factory.create(any())).thenReturn(TestWritableMessage(byteArray, 5, byteArray.size - 5))

        val stream = ByteArrayOutputStream()
        val channel = BufferedSendActionChannel(RingChannel.Type.NODE_ACCEPT, Channels.newChannel(stream), factory, mapper)

        channel.queue(action)

        assertFalse(channel.send())
        assertTrue(channel.send())

        assertEquals(action, mapper.toObject(extractByteArray(stream.toByteArray())))
    }

    @Test
    fun sendAction__single_action() {
        val action = Action.ClusterAction.Join(node)
        val stream = ByteArrayOutputStream()

        val channel = BufferedSendActionChannel(RingChannel.Type.NODE_ACCEPT, Channels.newChannel(stream), factory, mapper)
        channel.queue(action)
        channel.send()

        assertEquals(action, mapper.toObject(extractByteArray(stream.toByteArray())))
    }

    @Test
    fun sendAction__multiple_action() {
        val join = Action.ClusterAction.Join(node)
        val leave = Action.ClusterAction.Leave(node)
        val drop = Action.ClusterAction.Drop(node)
        val stream = ByteArrayOutputStream()

        val channel = BufferedSendActionChannel(RingChannel.Type.NODE_ACCEPT, Channels.newChannel(stream), factory, mapper)
        channel.queue(join)
        channel.queue(leave)
        channel.queue(drop)

        while (channel.send()) {
        }

        assertEquals(listOf(join, leave, drop), getActions(stream.toByteArray()))
    }

    private fun getActions(byteArray: ByteArray): List<Action> {
        var pos = 0

        val actions = mutableListOf<Action>()
        while (pos + MESSAGE_HEADER_SIZE < byteArray.size - 1) {
            val headerBytes = byteArray.sliceArray(pos.until(pos + MESSAGE_HEADER_SIZE))
            val headerBuffer = ByteBuffer.wrap(headerBytes)
            val bodySize = headerBuffer.getShort(0)

            pos += MESSAGE_HEADER_SIZE

            val bodyBytes = byteArray.sliceArray(pos.until(pos + bodySize))
            val bodyBuffer = ByteBuffer.wrap(bodyBytes)

            pos += bodySize

            actions.add(mapper.toObject(bodyBuffer.array()))
        }

        return actions
    }
}