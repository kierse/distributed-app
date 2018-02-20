package eece513.fs

import eece513.fs.Actions
import eece513.fs.message.SendableMessageFactory
import eece513.fs.model.Node
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.*
import java.nio.channels.WritableByteChannel
import kotlin.concurrent.thread
import kotlin.coroutines.experimental.CoroutineContext

class PredecessorHeartbeatMonitorController(
        private val heartbeatChannel: ReceiveChannel<Heartbeat>,
        private val sendableMessageFactory: SendableMessageFactory,
        private val logger: Logger
) {
    private class PredecessorHeartbeatMonitor(
            private val predecessors: List<Node>,
            private val writableByteChannel: WritableByteChannel,
            private val heartbeatChannel: ReceiveChannel<Heartbeat>,
            private val sendableMessageFactory: SendableMessageFactory,
            private val logger: Logger
    ): () -> Unit {
        private val tag = PredecessorHeartbeatMonitor::class.java.simpleName

        private val nodeToChannel = mutableMapOf<Node, SendChannel<Boolean>>()
        private val channelToJob = mutableMapOf<SendChannel<Boolean>, Job>()

        override fun invoke() {
            logger.info(tag, "starting predecessor heartbeat monitor!")

            try {
                runBlocking {
                    for (node in predecessors) {
                        val channel = Channel<Boolean>()
                        nodeToChannel[node] = channel

                        val job = createWatcher(coroutineContext, node, channel)
                        channelToJob[channel] = job

                        job.invokeOnCompletion {
                            logger.debug(tag, "cleaning up $node watcher")
                            nodeToChannel.remove(node)
                            channelToJob.remove(channel)
                        }

                        logger.info(tag, "monitoring heartbeats from ${node.addr}")
                    }

                    while (isActive) {
                        // Attention: this will block until the event channel has data!
                        val heartbeat = heartbeatChannel.receive()

                        val node = heartbeat.node
                        val channel = nodeToChannel[node]

                        if (channel != null) {
                            logger.debug(tag, "alerting watcher that heartbeat has been received...")
                            channel.send(true)
                            continue
                        }

                        logger.warn(tag, "received heartbeat notification for unknown node: $node. Ignoring!")
                    }
                }
            } catch (_: InterruptedException) {
                logger.info(tag, "predecessor monitor halted")
            }
        }

        private fun createWatcher(context: CoroutineContext, node: Node, channel: ReceiveChannel<Boolean>) =
                launch(context = context) {
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
                        val message = Actions.Membership.newBuilder()
                                .setTimestamp(timestamp)
                                .setHostName(address.hostName)
                                .setPort(address.port)
                                .build()
                                .toByteArray()

                        logger.warn(tag, "no heartbeat for $address!")

                        // Note: this will block until entire message has been sent
                        val sendableMessage = sendableMessageFactory.create(message)
                        do {
                            sendableMessage.send(writableByteChannel)
                        } while (!sendableMessage.complete)

                        // shutdown channel
                        logger.debug(tag, "shutting down channel for $address")
                        break
                    }

                    logger.debug(tag, "halting heartbeat monitor for ${node.addr}")
                }
    }

    data class Heartbeat(val node: Node)

    private var monitorThread: Thread? = null

    fun start(
            writableByteChannel: WritableByteChannel,
            predecessors: List<Node>
    ) {
        // there can be only one...
        if (monitorThread != null) throw IllegalStateException("there can be only one!")

        monitorThread = thread(name = "HEARTBEAT-WATCHER", isDaemon = true, block = PredecessorHeartbeatMonitor(
                predecessors, writableByteChannel, heartbeatChannel, sendableMessageFactory, logger
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