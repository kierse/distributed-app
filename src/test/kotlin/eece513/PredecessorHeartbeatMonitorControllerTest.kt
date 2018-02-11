package eece513

import eece513.model.Node
import org.junit.Test

import org.junit.Assert.*
import java.net.*
import java.nio.channels.DatagramChannel
import java.nio.channels.Pipe
import java.time.Instant

class PredecessorHeartbeatMonitorControllerTest {
    private val addr = Inet4Address.getByName("127.0.0.1")
    private val serverAddr = InetSocketAddress(addr, 6970)
    private val localAddr = InetSocketAddress(addr, 6971)
    private val node = Node(localAddr, Instant.now())
    private val logger = TinyLogWrapper()

    @Test
    fun start__no_heartbeats() {
        val expected = Message.Predecessor.newBuilder()
                .setHostName(localAddr.hostString)
                .setPort(localAddr.port)
                .build()

        DatagramChannel
                .open()
                .bind(serverAddr)
                .use { socket ->
                    val pipe = Pipe.open()
                    val sink = pipe.sink()
                    val source = pipe.source()

                    val controller = PredecessorHeartbeatMonitorController(
                            socket, sink, MessageBuilder(), logger
                    )
                    controller.start(listOf(node))

                    val byteArray = MessageReader(logger).read(source)
                    val result = Message.Predecessor.parseFrom(byteArray)

                    assertEquals(expected, result)

                    controller.stop()
                }
    }
    @Test
    fun start__node_crash_after_one_heartbeat() {
        val expected = Message.Predecessor.newBuilder()
                .setHostName(localAddr.hostString)
                .setPort(localAddr.port)
                .build()

        DatagramChannel
                .open()
                .bind(serverAddr)
                .use { channel ->
                    val pipe = Pipe.open()
                    val sink = pipe.sink()
                    val source = pipe.source()

                    val controller = PredecessorHeartbeatMonitorController(
                            channel, sink, MessageBuilder(), logger
                    )
                    controller.start(listOf(node))

                    DatagramSocket(localAddr).use { socket ->
                        socket.send(DatagramPacket(ByteArray(0), 0, serverAddr))
                    }

                    val byteArray = MessageReader(logger).read(source)
                    val result = Message.Predecessor.parseFrom(byteArray)

                    assertEquals(expected, result)

                    controller.stop()
                }
    }

    @Test
    fun start__node_crash_after_two_heartbeat() {
        val expected = Message.Predecessor.newBuilder()
                .setHostName(localAddr.hostString)
                .setPort(localAddr.port)
                .build()

        DatagramChannel
                .open()
                .bind(serverAddr)
                .use { channel ->
                    val pipe = Pipe.open()
                    val sink = pipe.sink()
                    val source = pipe.source()

                    val controller = PredecessorHeartbeatMonitorController(
                            channel, sink, MessageBuilder(), logger
                    )
                    controller.start(listOf(node))

                    val iterator = (0..1).iterator()
                    do {
                        iterator.next()
                        DatagramSocket(localAddr).use { socket ->
                            socket.send(DatagramPacket(ByteArray(0), 0, serverAddr))
                        }
                        if (iterator.hasNext()) Thread.sleep(HEARTBEAT_INTERVAL)
                    } while (iterator.hasNext())

                    val byteArray = MessageReader(logger).read(source)
                    val result = Message.Predecessor.parseFrom(byteArray)

                    assertEquals(expected, result)

                    controller.stop()
                }
    }

    @Test
    fun start__two_nodes_one_failure() {
        val secondArr = InetSocketAddress(addr, 6972)
        val secondNode = Node(secondArr, Instant.now())

        val expected = Message.Predecessor.newBuilder()
                .setHostName(secondArr.hostString)
                .setPort(secondArr.port)
                .build()

        DatagramChannel
                .open()
                .bind(serverAddr)
                .use { channel ->
                    val pipe = Pipe.open()
                    val sink = pipe.sink()
                    val source = pipe.source()

                    val controller = PredecessorHeartbeatMonitorController(
                            channel, sink, MessageBuilder(), logger
                    )
                    controller.start(listOf(node, secondNode))

                    DatagramSocket(localAddr).use { socket ->
                        socket.send(DatagramPacket(ByteArray(0), 0, serverAddr))
                    }

                    val byteArray = MessageReader(logger).read(source)
                    val result = Message.Predecessor.parseFrom(byteArray)

                    assertEquals(expected, result)

                    controller.stop()
                }
    }

    @Test
    fun start__two_nodes_two_failures() {
        val secondArr = InetSocketAddress(addr, 6972)
        val secondNode = Node(secondArr, Instant.now())

        val expected = mutableSetOf<Message.Predecessor>()
        expected.add(
                Message.Predecessor.newBuilder()
                        .setHostName(localAddr.hostString)
                        .setPort(localAddr.port)
                        .build()
        )
        expected.add(
                Message.Predecessor.newBuilder()
                        .setHostName(secondArr.hostString)
                        .setPort(secondArr.port)
                        .build()
        )

        DatagramChannel
                .open()
                .bind(serverAddr)
                .use { channel ->
                    val pipe = Pipe.open()
                    val sink = pipe.sink()
                    val source = pipe.source()

                    val controller = PredecessorHeartbeatMonitorController(
                            channel, sink, MessageBuilder(), logger
                    )
                    controller.start(listOf(node, secondNode))

                    val result = mutableSetOf<Message.Predecessor>()
                    result.add(Message.Predecessor.parseFrom(MessageReader(logger).read(source)))
                    result.add(Message.Predecessor.parseFrom(MessageReader(logger).read(source)))

                    assertEquals(expected, result)

                    controller.stop()
                }
    }
}