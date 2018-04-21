package eece513.fs.channel

import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.whenever
import eece513.common.model.Action
import eece513.fs.mapper.ClusterActionMapper
import eece513.fs.message.ReadableMessageFactory
import eece513.common.model.Node
import eece513.fs.DummyLogger
import org.junit.Test

import org.junit.Assert.*
import java.io.ByteArrayInputStream
import java.net.InetSocketAddress
import java.nio.channels.Channels
import java.time.Instant

class ReadObjectChannelTest {
    private val node = Node(InetSocketAddress("127.0.0.1", 6969), Instant.now())
    private val factory = ReadableMessageFactory()
    private val mapper = ClusterActionMapper()

    @Test
    fun getType() {
        assertEquals(
                RingChannel.Type.NODE_ACCEPT,
                ReadObjectChannel(RingChannel.Type.NODE_ACCEPT, mock(), factory, mapper, DummyLogger()).type
        )
    }

    @Test
    fun read() {
        val action = Action.ClusterAction.Join(node)
        val channel = ReadObjectChannel(
                RingChannel.Type.NODE_ACCEPT,
                buildReadableByteChannel(mapper.toByteArray(action)),
                factory,
                mapper,
                DummyLogger()
        )

        assertEquals(action, channel.read())
    }

    @Test
    fun read__received_partial_message() {
        val action = Action.ClusterAction.Join(node)
        val byteArray = mapper.toByteArray(action)
        val stream = ByteArrayInputStream(byteArray)
        val readChannel = Channels.newChannel(stream)

        val factory = mock<ReadableMessageFactory>()
        whenever(factory.create()).thenReturn(TestReadableMessage(5, byteArray.size - 5))

        val channel = ReadObjectChannel(
                RingChannel.Type.NODE_ACCEPT, readChannel, factory, mapper, DummyLogger()
        )

        assertNull(channel.read())

        assertEquals(action, channel.read())
    }
}