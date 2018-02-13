package eece513

import eece513.model.Node
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.SendChannel
import java.nio.channels.WritableByteChannel
import kotlin.concurrent.thread

class PredecessorHeartbeatMonitorController(
        private val messageBuilder: MessageBuilder,
        private val logger: Logger
) {
    private class PredecessorHeartbeatMonitor(
            predecessors: List<Node>,
            private val writableByteChannel: WritableByteChannel,
            private val heartbeatChannel: ReceiveChannel<Heartbeat>,
            private val messageBuilder: MessageBuilder,
            private val logger: Logger
    ): () -> Unit {
        private val tag = PredecessorHeartbeatMonitor::class.java.simpleName

        private val nodeToChannel = mutableMapOf<Node, SendChannel<Boolean>>()

        init {
            logger.info(tag, "starting predecessor heartbeat monitor!")
            for (node in predecessors) {
                val channel = Channel<Boolean>()
                createWatcher(node, channel)
                nodeToChannel[node] = channel

                logger.info(tag, "monitoring heartbeats from ${node.addr}")
            }
        }

        override fun invoke() {
            try {
                runBlocking {
                    while (isActive) {
                        // Attention: this will block until the event channel has data!
                        val heartbeat = heartbeatChannel.receive()
                        val address = heartbeat.node

                        val channel = nodeToChannel[address]
                        if (channel != null) {
                            channel.send(true)
                            continue
                        }

                        logger.warn(tag, "received heartbeat notification for unknown address: $address. Ignoring!")
                    }
                }
            } catch (_: InterruptedException) {
                // do nothing
            }
        }

        private fun createWatcher(node: Node, channel: ReceiveChannel<Boolean>) =
                launch {
                    while (isActive) {
                        val heartbeat = withTimeoutOrNull(HEARTBEAT_TIMEOUT) {
                            channel.receive() // suspends until channel has data
                        } ?: false

                        if (heartbeat) {
                            logger.debug(tag, "heartbeat received from ${node.addr}")
                            continue
                        }

                        // terminate early if our coroutine is no longer active
                        if (!isActive) break

                        val timestamp = Actions.Timestamp.newBuilder()
                                .setSecondsSinceEpoch(node.joinedAt.epochSecond)
                                .setNanoSeconds(node.joinedAt.nano)
                                .build()

                        val address = node.addr
                        val message = Actions.Request.newBuilder()
                                .setType(Actions.Request.Type.DROP)
                                .setHostName(address.hostString)
                                .setPort(address.port)
                                .setTimestamp(timestamp)
                                .build()
                                .toByteArray()

                        logger.warn(tag, "no heartbeat for $address!")
                        writableByteChannel.write(
                                messageBuilder.build(message)
                        )

                        // shutdown channel
                        logger.debug(tag, "shutting down channel for $address")
                        channel.cancel()
                        break
                    }

                    logger.debug(tag, "halting heartbeat monitor for ${node.addr}")
                }
    }

    class Heartbeat(val node: Node)

    private var monitorThread: Thread? = null

    fun start(
            writableByteChannel: WritableByteChannel,
            heartbeatChannel: ReceiveChannel<Heartbeat>,
            predecessors: List<Node>
    ) {
        // there can be only one...
        if (monitorThread != null) throw IllegalStateException("there can be only one!")

        monitorThread = thread(name = "HEARTBEAT-WATCHER", isDaemon = true, block = PredecessorHeartbeatMonitor(
                predecessors, writableByteChannel, heartbeatChannel, messageBuilder, logger
        ))
    }

    fun stop() {
        monitorThread?.apply {
            interrupt()
            join()
        }
        monitorThread = null
    }
}