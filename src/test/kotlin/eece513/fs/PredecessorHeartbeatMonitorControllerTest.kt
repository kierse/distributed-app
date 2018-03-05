package eece513.fs

import eece513.fs.channel.MissedHeartbeatChannel
import eece513.fs.mapper.NodeMapper
import eece513.fs.message.ReadableMessageFactory
import eece513.fs.message.SendableMessageFactory
import eece513.fs.model.Node
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
        val pipe = Pipe.open()
        val channel = Channel<PredecessorHeartbeatMonitorController.Heartbeat>(1)

        val controller = PredecessorHeartbeatMonitorController(channel, SendableMessageFactory(), logger)
        controller.start(pipe.sink(), listOf(node))

        val missedHeartbeatChannel =  MissedHeartbeatChannel(pipe.source(), ReadableMessageFactory(), NodeMapper(), DummyLogger())

        try {
            assertEquals(node, missedHeartbeatChannel.read())
        } finally {
            controller.stop()
        }
    }

    @Test
    fun start__one_heartbeat() = runBlocking {
        val pipe = Pipe.open()
        val channel = Channel<PredecessorHeartbeatMonitorController.Heartbeat>(1)

        val controller = PredecessorHeartbeatMonitorController(channel, SendableMessageFactory(), logger)
        controller.start(pipe.sink(), listOf(node))

        channel.send(PredecessorHeartbeatMonitorController.Heartbeat(node))

        val missedHeartbeatChannel =  MissedHeartbeatChannel(pipe.source(), ReadableMessageFactory(), NodeMapper(), DummyLogger())

        try {
            assertEquals(node, missedHeartbeatChannel.read())
        } finally {
            controller.stop()
        }
    }

    @Test
    fun start__two_heartbeats() = runBlocking {
        val pipe = Pipe.open()
        val channel = Channel<PredecessorHeartbeatMonitorController.Heartbeat>(1)

        val controller = PredecessorHeartbeatMonitorController(channel, SendableMessageFactory(), logger)
        controller.start(pipe.sink(), listOf(node))

        channel.send(PredecessorHeartbeatMonitorController.Heartbeat(node))
        delay(HEARTBEAT_INTERVAL)
        channel.send(PredecessorHeartbeatMonitorController.Heartbeat(node))

        val missedHeartbeatChannel =  MissedHeartbeatChannel(pipe.source(), ReadableMessageFactory(), NodeMapper(), DummyLogger())

        try {
            assertEquals(node, missedHeartbeatChannel.read())
        } finally {
            controller.stop()
        }
    }

//    @Test
//    fun start__two_nodes_one_heartbeat() = runBlocking {
//        val node2 = Node(InetSocketAddress(addr, 6972), Instant.now())
//        val expected = buildProtoRequest(node2)
//
//        val pipe = Pipe.open()
//        val channel = Channel<PredecessorHeartbeatMonitorController.Heartbeat>(1)
//        val controller = PredecessorHeartbeatMonitorController(MessageBuilder(), logger)
//
//        controller.start(pipe.sink(), channel, listOf(node, node2))
//
//        channel.send(PredecessorHeartbeatMonitorController.Heartbeat(node))
//
//        val byteArray = MessageReader(logger).read(pipe.source())
//        val result = Actions.Membership.parseFrom(byteArray)
//
//        assertEquals(expected, result)
//
//        controller.stop()
//    }

    @Test
    fun start__two_nodes_three_heartbeats_one_miss() = runBlocking {
        val node2 = Node(InetSocketAddress(addr, 6972), Instant.now())

        val pipe = Pipe.open()
        val channel = Channel<PredecessorHeartbeatMonitorController.Heartbeat>(1)

        val controller = PredecessorHeartbeatMonitorController(channel, SendableMessageFactory(), logger)
        controller.start(pipe.sink(), listOf(node, node2))

        channel.send(PredecessorHeartbeatMonitorController.Heartbeat(node))
        channel.send(PredecessorHeartbeatMonitorController.Heartbeat(node2))
        delay(HEARTBEAT_INTERVAL)
        channel.send(PredecessorHeartbeatMonitorController.Heartbeat(node))

        val missedHeartbeatChannel =  MissedHeartbeatChannel(pipe.source(), ReadableMessageFactory(), NodeMapper(), DummyLogger())

        try {
            assertEquals(node2, missedHeartbeatChannel.read())
        } finally {
            controller.stop()
        }
    }

    @Test
    fun start__two_nodes_two_misses() = runBlocking {
        val node2 = Node(InetSocketAddress(addr, 6972), Instant.now())
        val expected = setOf(node, node2)

        val pipe = Pipe.open()
        val channel = Channel<PredecessorHeartbeatMonitorController.Heartbeat>(1)

        val controller = PredecessorHeartbeatMonitorController(channel, SendableMessageFactory(), logger)
        controller.start(pipe.sink(), listOf(node, node2))

        delay(HEARTBEAT_TIMEOUT)

        val missedHeartbeatChannel =  MissedHeartbeatChannel(pipe.source(), ReadableMessageFactory(), NodeMapper(), DummyLogger())
        val result1 = missedHeartbeatChannel.read()
        val result2 = missedHeartbeatChannel.read()

        try {
            assertEquals(expected, setOf(result1, result2))
        } finally {
            controller.stop()
        }
    }
}