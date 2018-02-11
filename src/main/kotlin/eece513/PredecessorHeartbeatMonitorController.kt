package eece513

import eece513.model.Node
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.WritableByteChannel
import kotlin.concurrent.thread

class PredecessorHeartbeatMonitorController(
        private val datagramChannel: DatagramChannel,
        private val writableByteChannel: WritableByteChannel,
        private val messageBuilder: MessageBuilder,
        private val logger: Logger
) {
    private class PredecessorHeartbeatMonitor(
            predecessors: List<Node>,
            private val datagramChannel: DatagramChannel,
            private val writableByteChannel: WritableByteChannel,
            private val messageBuilder: MessageBuilder,
            private val logger: Logger
    ): () -> Unit {
        private val tag = PredecessorHeartbeatMonitor::class.java.simpleName

        private val emptyBuffer = ByteBuffer.allocate(0)
        private val addressToChannel: Map<SocketAddress, Channel<Boolean>>
        private val jobs: Map<SocketAddress, Job>

        init {
            addressToChannel = mutableMapOf()
            jobs = mutableMapOf()

            predecessors
                    .map { it.addr to Channel<Boolean>() }
                    .forEach { (addr, channel) ->
                        addressToChannel[addr] = channel
                        jobs[addr] = watcher(addr as InetSocketAddress, channel)
                    }

            logger.debug(tag, "jobs: ${jobs.size}, channels: ${addressToChannel.size}, nodes: ${predecessors.size}")
        }

        override fun invoke() = runBlocking {
            // start watchers
            logger.debug(tag, "starting predecessor monitors")
            jobs.values.forEach { it.start() }

            datagramChannel.configureBlocking(false)

            Selector.open().use { selector ->
                datagramChannel.register(selector, SelectionKey.OP_READ)

                logger.debug(tag, "waiting for heartbeats on ${datagramChannel.localAddress}")
                while (true) {
                    selector.select()

                    if (Thread.currentThread().isInterrupted) {
                        logger.debug(tag, "thread interrupted, bailing out!")
                        break
                    }

                    val selectionKeySet = selector.selectedKeys()
                    for (key in selectionKeySet) {
                        val remoteAddr = datagramChannel.receive(emptyBuffer)
                        logger.info(tag, "received heartbeat from $remoteAddr")

                        val channel = addressToChannel.getValue(remoteAddr)
                        channel.send(true)

                        selectionKeySet.remove(key)
                    }
                }
            }
        }

        private fun watcher(addr: InetSocketAddress, channel: ReceiveChannel<Boolean>) =
                // Note: staring with LAZY flag. Must explicitly call start()
                launch(start = CoroutineStart.LAZY) {
                    while (true) {
                        val heartbeat = withTimeoutOrNull(HEARTBEAT_TIMEOUT) {
                            channel.receive() // suspends until channel has data
                        } ?: false

                        if (heartbeat) {
                            logger.debug(tag, "timeout cancelled, heartbeat received for $addr")
                            continue
                        }

                        val message = Message.Predecessor.newBuilder()
                                .setHostName(addr.hostString)
                                .setPort(addr.port)
                                .build()
                                .toByteArray()

                        logger.info(tag, "no heartbeat for $addr!")
                        writableByteChannel.write(
                                messageBuilder.build(message)
                        )

                        // shutdown channel
                        logger.debug(tag, "shutting down channel for $addr")
                        channel.cancel()
                        break
                    }

                    logger.debug(tag, "halting monitor job for $addr")
                }
    }

    private var monitorThread: Thread? = null

    fun start(nodes: List<Node>) {
        val runningThread = monitorThread
        if (runningThread != null) {
            runningThread.interrupt()
            runningThread.join()
        }

        monitorThread = thread(block = PredecessorHeartbeatMonitor(
                nodes, datagramChannel, writableByteChannel, messageBuilder, logger
        ))
    }

    fun stop() {
        monitorThread?.interrupt()
        monitorThread?.join()
    }
}