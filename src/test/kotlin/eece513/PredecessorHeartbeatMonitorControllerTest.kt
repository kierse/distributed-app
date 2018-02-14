package eece513

import eece513.model.Node
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import org.junit.Test

import org.junit.Assert.*
import java.net.*
import java.nio.channels.Pipe
import java.time.Instant

class PredecessorHeartbeatMonitorControllerTest {
    private val addr = Inet4Address.getByName("127.0.0.1")
    private val localAddr = InetSocketAddress(addr, 6971)
    private val node = Node(localAddr, Instant.now())
    private val logger = TinyLogWrapper()

    @Test
    fun start__no_heartbeats() {
        val expected = buildProtoRequest(node)

        val pipe = Pipe.open()
        val channel = Channel<PredecessorHeartbeatMonitorController.Heartbeat>(1)

        val controller = PredecessorHeartbeatMonitorController(MessageBuilder(), logger)
        controller.start(pipe.sink(), channel, listOf(node))

        val byteArray = MessageReader(logger).read(pipe.source())
        val result = Actions.Request.parseFrom(byteArray)

        assertEquals(expected, result)

        controller.stop()
    }

    @Test
    fun start__one_heartbeat() = runBlocking {
        val expected = buildProtoRequest(node)

        val pipe = Pipe.open()
        val channel = Channel<PredecessorHeartbeatMonitorController.Heartbeat>(1)
        val controller = PredecessorHeartbeatMonitorController(MessageBuilder(), logger)

        controller.start(pipe.sink(), channel, listOf(node))
        channel.send(PredecessorHeartbeatMonitorController.Heartbeat(node))

        val byteArray = MessageReader(logger).read(pipe.source())
        val result = Actions.Request.parseFrom(byteArray)

        assertEquals(expected, result)

        controller.stop()
    }

    @Test
    fun start__two_heartbeats() = runBlocking {
        val expected = buildProtoRequest(node)

        val pipe = Pipe.open()
        val channel = Channel<PredecessorHeartbeatMonitorController.Heartbeat>(1)
        val controller = PredecessorHeartbeatMonitorController(MessageBuilder(), logger)

        controller.start(pipe.sink(), channel, listOf(node))

        channel.send(PredecessorHeartbeatMonitorController.Heartbeat(node))
        delay(HEARTBEAT_INTERVAL)
        channel.send(PredecessorHeartbeatMonitorController.Heartbeat(node))

        val byteArray = MessageReader(logger).read(pipe.source())
        val result = Actions.Request.parseFrom(byteArray)

        assertEquals(expected, result)

        controller.stop()
    }

    @Test
    fun start__two_nodes_one_heartbeat() = runBlocking {
        val node2 = Node(InetSocketAddress(addr, 6972), Instant.now())
        val expected = buildProtoRequest(node2)

        val pipe = Pipe.open()
        val channel = Channel<PredecessorHeartbeatMonitorController.Heartbeat>(1)
        val controller = PredecessorHeartbeatMonitorController(MessageBuilder(), logger)

        controller.start(pipe.sink(), channel, listOf(node, node2))

        channel.send(PredecessorHeartbeatMonitorController.Heartbeat(node))

        val byteArray = MessageReader(logger).read(pipe.source())
        val result = Actions.Request.parseFrom(byteArray)

        assertEquals(expected, result)

        controller.stop()
    }

    @Test
    fun start__two_nodes_three_heartbeats_one_miss() = runBlocking {
        val node2 = Node(InetSocketAddress(addr, 6972), Instant.now())
        val expected = buildProtoRequest(node2)

        val pipe = Pipe.open()
        val channel = Channel<PredecessorHeartbeatMonitorController.Heartbeat>(1)
        val controller = PredecessorHeartbeatMonitorController(MessageBuilder(), logger)

        controller.start(pipe.sink(), channel, listOf(node, node2))

        channel.send(PredecessorHeartbeatMonitorController.Heartbeat(node))
        channel.send(PredecessorHeartbeatMonitorController.Heartbeat(node2))
        delay(HEARTBEAT_INTERVAL)
        channel.send(PredecessorHeartbeatMonitorController.Heartbeat(node))

        val byteArray = MessageReader(logger).read(pipe.source())
        val result = Actions.Request.parseFrom(byteArray)

        assertEquals(expected, result)

        controller.stop()
    }

    @Test
    fun start__two_nodes_two_misses() = runBlocking {
        val node2 = Node(InetSocketAddress(addr, 6972), Instant.now())
        val expected = setOf(buildProtoRequest(node2), buildProtoRequest(node))

        val pipe = Pipe.open()
        val channel = Channel<PredecessorHeartbeatMonitorController.Heartbeat>(1)
        val controller = PredecessorHeartbeatMonitorController(MessageBuilder(), logger)

        controller.start(pipe.sink(), channel, listOf(node, node2))

        delay(HEARTBEAT_INTERVAL)

        val result1 = Actions.Request.parseFrom(
                MessageReader(logger).read(pipe.source())
        )
        val result2 = Actions.Request.parseFrom(
                MessageReader(logger).read(pipe.source())
        )

        assertEquals(expected, setOf(result1, result2))

        controller.stop()
    }

    private fun buildProtoRequest(node: Node): Actions.Request {
        val timestamp = Actions.Timestamp.newBuilder()
                .setSecondsSinceEpoch(node.joinedAt.epochSecond)
                .setNanoSeconds(node.joinedAt.nano)
                .build()

        return Actions.Request.newBuilder()
                .setHostName(node.addr.hostString)
                .setPort(node.addr.port)
                .setType(Actions.Request.Type.DROP)
                .setTimestamp(timestamp)
                .build()
    }
}