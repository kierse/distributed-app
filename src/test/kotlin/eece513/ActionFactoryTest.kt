package eece513

import eece513.model.Action
import eece513.model.Node
import org.junit.Assert.*
import org.junit.Test
import java.io.ByteArrayInputStream
import java.net.Inet4Address
import java.net.InetSocketAddress
import java.nio.channels.Channels
import java.time.Instant

class ActionFactoryTest {
    private val addr = Inet4Address.getByName("127.0.0.1")
    private val logger = DummyLogger()
    private val messageBuilder = MessageBuilder()
    private val messageReader = MessageReader(logger)

    @Test
    fun build__one_action() {
        val expected = listOf(createAction())

        val request = createActionRequest(expected.first())
        val message = messageBuilder.build(request.toByteArray())

        val stream = ByteArrayInputStream(message.array())
        val channel = Channels.newChannel(stream)

        val result = ActionFactory(messageReader, logger)
                .build(channel)

        assertEquals(expected, result)
    }

    @Test
    fun build__two_actions() {
        val expected = listOf(
                createAction(6970),
                createAction(6971)
        )

        val requests = expected.map { createActionRequest(it) }
        val message = requests.map { messageBuilder.build(it.toByteArray()) }
                              .let { messageBuilder.combineMessages(it) }

        val stream = ByteArrayInputStream(message.array())
        val channel = Channels.newChannel(stream)

        val result = ActionFactory(messageReader, logger)
                .build(channel)

        assertEquals(expected, result)
    }

    private fun createActionRequest(action: Action): Actions.Request {
        val timestamp = Actions.Timestamp.newBuilder()
                .setSecondsSinceEpoch(action.node.joinedAt.epochSecond)
                .setNanoSeconds(action.node.joinedAt.nano)
                .build()

        val builder = Actions.Request.newBuilder()
                .setTimestamp(timestamp)
                .setHostName(action.node.addr.hostString)
                .setPort(action.node.addr.port)

        val type = when (action.type) {
            Action.Type.JOIN -> Actions.Request.Type.JOIN
            Action.Type.LEAVE -> Actions.Request.Type.REMOVE
            Action.Type.DROP -> Actions.Request.Type.DROP
        }

        return builder
                .setType(type)
                .build()
    }

    private fun createAction(port: Int = 6970): Action {
        return Action.Join(
                Node(
                        InetSocketAddress(addr, port),
                        Instant.now()
                )
        )
    }
}