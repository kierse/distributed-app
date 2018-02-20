package eece513.fs

import eece513.fs.channel.*
import eece513.fs.mapper.ActionMapper
import eece513.fs.mapper.MembershipListMapper
import eece513.fs.mapper.NodeMapper
import eece513.fs.message.ReadableMessageFactory
import eece513.fs.message.SendableMessageFactory
import eece513.fs.model.Action
import eece513.fs.model.Node
import eece513.fs.ring.Ring
import eece513.fs.ring.RingImpl
import kotlinx.coroutines.experimental.channels.Channel
import java.io.IOException
import java.net.*
import java.nio.channels.*
import java.time.Instant
import java.util.*
import kotlin.concurrent.scheduleAtFixedRate

fun main(args: Array<String>) {
    val logger = TinyLogWrapper(LOG_LOCATION)

    val localAddr = InetAddress.getLocalHost()
//    val localAddr = InetAddress.getByName("127.0.0.1")
    val socketAddr: InetSocketAddress = ServerSocket(0).use { InetSocketAddress(localAddr, it.localPort) }
    val self = Node(socketAddr, Instant.now())

    val heartbeatCoroutineChannel = Channel<PredecessorHeartbeatMonitorController.Heartbeat>(3)
    val ring: Ring = RingImpl(self, heartbeatCoroutineChannel, logger)
    val predecessorMonitor = PredecessorHeartbeatMonitorController(heartbeatCoroutineChannel, SendableMessageFactory(), logger)

    val node = ClusterNode(
            localAddr, socketAddr, self, ring, predecessorMonitor, logger
    )

    var address: InetSocketAddress? = null
    var interval = 0L

    if (args.isNotEmpty()) {
        address = InetSocketAddress(InetAddress.getByName(args.first()), JOIN_PORT)
        if (args.size > 1) {
            interval = args[1].toLong()
        }
    }

    node.start(address, interval)
}

class ClusterNode(
        private val localAddr: InetAddress,
        private val socketAddr: InetSocketAddress,
        private val self: Node,
        private val ring: Ring,
        private val predecessorMonitor: PredecessorHeartbeatMonitorController,
        private val logger: Logger
) {

    private val tag = ClusterNode::class.java.simpleName

    private val timer = Timer("HEARTBEAT-TIMER", true)
    private var heartbeatTimerTask: TimerTask? = null

    private val successorChannels = mutableMapOf<SelectionKey, Node>()
    private val predecessorChannels = mutableMapOf<SelectionKey, Node>()

    private val heartbeatByteArray = ActionMapper().toByteArray(Action.Heartbeat(self))

    fun start(address: SocketAddress?, interval: Long) {
        println("Join address: ${localAddr.hostName}")

        var terminateAround: Instant? = null
        if (interval > 0L) {
            terminateAround = Instant.now().plusSeconds(interval)
        }

        logger.info(tag, "self node: $self")

        val sendableMessageFactory = SendableMessageFactory()
        val readableMessageFactory = ReadableMessageFactory()

        val actionMapper = ActionMapper()
        val membershipListMapper = MembershipListMapper()
        val nodeMapper = NodeMapper()

        Selector.open().use { selector ->
            logger.info(tag, "listening for TCP connections on $socketAddr")
            val successorServerChannel = ServerSocketChannel.open().bind(socketAddr)
            successorServerChannel.configureBlocking(false)
            successorServerChannel.register(selector, SelectionKey.OP_ACCEPT, RingChannelImpl(RingChannel.Type.SUCCESSOR_ACCEPT))

            logger.info(tag, "listening for UDP connections on $socketAddr")
            val predecessorHeartbeatChannel = DatagramChannel.open().bind(socketAddr)
            val heartbeatChannel = ReadHeartbeatChannel(
                    predecessorHeartbeatChannel, actionMapper, logger
            )

            predecessorHeartbeatChannel.configureBlocking(false)
            predecessorHeartbeatChannel.register(selector, SelectionKey.OP_READ, heartbeatChannel)

            val pipe = Pipe.open()
            val predecessorMissedHeartbeatChannel = pipe.source()
            val missedHeartbeatChannel = MissedHeartbeatChannel(
                    predecessorMissedHeartbeatChannel, readableMessageFactory, nodeMapper, logger
            )

            predecessorMissedHeartbeatChannel.configureBlocking(false)
            predecessorMissedHeartbeatChannel.register(selector, SelectionKey.OP_READ, missedHeartbeatChannel)

            val joinRequestServerChannel = ServerSocketChannel.open().bind(InetSocketAddress(localAddr, JOIN_PORT))
            joinRequestServerChannel?.configureBlocking(false)
            joinRequestServerChannel?.register(selector, SelectionKey.OP_ACCEPT, RingChannelImpl(RingChannel.Type.JOIN_ACCEPT))
            logger.info(tag, "listening for joins on ${joinRequestServerChannel.socket().localSocketAddress}")

            if (address != null) {
                val joinClusterChannel = SocketChannel.open()
                joinClusterChannel.configureBlocking(false)
                joinClusterChannel.register(selector, SelectionKey.OP_CONNECT, RingChannelImpl(RingChannel.Type.JOIN_CONNECT))
                joinClusterChannel.connect(address)
                logger.debug(tag, "connecting to join server!")
            }

            var shouldILeaveRing = false
            while (selector.select() >= 0) {
                val keyIterator = selector.selectedKeys().iterator()

                if (!shouldILeaveRing && terminateAround != null && terminateAround < Instant.now()) {
                    logger.info(tag, "leaving cluster!")
                    disconnectFromPredecessors()
                    shouldILeaveRing = true

                    ring.leave()
                }

                while (keyIterator.hasNext()) {
                    val key = keyIterator.next()
                    keyIterator.remove()

                    // While processing a previous key, it is possible the channel associated with this key was closed
                    // Verify that the key is still valid before attempting to use it
                    if (!key.isValid) continue

                    val ringChannel = key.attachment() as RingChannel
                    when {
                        key.isConnectable -> when (ringChannel.type) {
                            RingChannel.Type.JOIN_CONNECT -> {
                                val channel = key.channel() as SocketChannel
                                if (channel.finishConnect()) {
                                    val sendChannel = SendActionChannel(
                                            RingChannel.Type.JOIN_CONNECT_WRITE, channel, sendableMessageFactory, actionMapper
                                    )

                                    key.interestOps(SelectionKey.OP_WRITE)
                                    key.attach(sendChannel)

                                    logger.debug(tag, "completed connection to join server!")
                                }
                            }

                            RingChannel.Type.PREDECESSOR_CONNECT -> {
                                val predecessor = predecessorChannels.getValue(key)
                                val channel = key.channel() as SocketChannel

                                try {
                                    if (channel.finishConnect()) {
                                        val sendChannel = SendActionChannel(
                                                RingChannel.Type.PREDECESSOR_CONNECT_WRITE, channel, sendableMessageFactory, actionMapper
                                        )

                                        key.interestOps(SelectionKey.OP_WRITE)
                                        key.attach(sendChannel)

                                        logger.debug(tag, "completed connection to predecessor at ${predecessor.addr}")
                                    }
                                } catch (_: IOException) {
                                    logger.warn(tag, "unable to establish connection to predecessor $predecessor. Dropping!")
                                    ring.dropPredecessor(predecessor)
                                }
                            }

                            else -> throw IllegalArgumentException("unknown CONNECT type: ${ringChannel.type}")
                        }

                        key.isAcceptable -> when (ringChannel.type) {
                            RingChannel.Type.JOIN_ACCEPT -> {
                                val serverChannel = key.channel() as ServerSocketChannel
                                val channel = serverChannel.accept()

                                val readActionChannel = ReadActionChannel(
                                        RingChannel.Type.JOIN_ACCEPT_READ, channel, readableMessageFactory, actionMapper, logger
                                )

                                channel.configureBlocking(false)
                                channel.register(selector, SelectionKey.OP_READ, readActionChannel)
                                logger.debug(tag, "accepting connection from new node at ${channel.remoteAddress}")
                            }

                            RingChannel.Type.SUCCESSOR_ACCEPT -> {
                                val serverChannel = key.channel() as ServerSocketChannel
                                val channel = serverChannel.accept()

                                val readChannel = ReadActionChannel(
                                        RingChannel.Type.SUCCESSOR_ACCEPT_READ, channel, readableMessageFactory, actionMapper, logger
                                )

                                channel.configureBlocking(false)
                                channel.register(selector, SelectionKey.OP_READ, readChannel)
                                logger.debug(tag, "accepting connection from successor at ${channel.remoteAddress}")
                            }

                            else -> throw IllegalArgumentException("unknown ACCEPT type: ${ringChannel.type}")
                        }

                        key.isReadable -> when (ringChannel.type) {
                            RingChannel.Type.PREDECESSOR_MISSED_HEARTBEAT_READ -> {
                                ring.processMissedHeartbeats(key.attachment() as MissedHeartbeatChannel)
                            }

                            RingChannel.Type.JOIN_ACCEPT_READ -> {
                                if (ring.processJoinRequest(key.attachment() as ReadActionChannel)) {
                                    val channel = key.channel() as SocketChannel
                                    val sendChannel = SendMembershipListChannel(
                                            RingChannel.Type.JOIN_ACCEPT_WRITE, channel, sendableMessageFactory, membershipListMapper
                                    )

                                    key.interestOps(SelectionKey.OP_WRITE)
                                    key.attach(sendChannel)
                                }
                            }

                            RingChannel.Type.JOIN_CONNECT_READ -> {
                                if (ring.readMembershipList(key.attachment() as ReadMembershipListChannel)) {
                                    val socketChannel = key.channel() as SocketChannel
                                    logger.debug(tag, "received membership list, closing JOIN connection to ${socketChannel.remoteAddress}")
                                    socketChannel.close()
                                }
                            }

                            RingChannel.Type.SUCCESSOR_ACCEPT_READ -> {
                                val successor = ring.readIdentity(key.attachment() as ReadActionChannel)
                                if (successor != null) {
                                    logger.debug(tag, "successor identified as $successor")
                                    successorChannels[key] = successor
                                    ring.addSuccessor(successor)

                                    heartbeatTimerTask?.cancel()
                                    startSendingHeartbeats()

                                    val channel = key.channel() as SocketChannel
                                    val sendChannel = BufferedSendActionChannel(
                                            RingChannel.Type.SUCCESSOR_ACTION_WRITE, channel, sendableMessageFactory, actionMapper
                                    )

                                    key.interestOps(SelectionKey.OP_WRITE)
                                    key.attach(sendChannel)
                                }
                            }

                            RingChannel.Type.PREDECESSOR_ACTION_READ -> {
                                val predecessor = predecessorChannels.getValue(key)
                                ring.processActionsFromPredecessor(predecessor, key.attachment() as ReadActionChannel)
                            }

                            RingChannel.Type.PREDECESSOR_HEARTBEAT_READ -> {
                                ring.processHeartbeat(key.attachment() as ReadHeartbeatChannel)
                            }

                            else -> throw IllegalArgumentException("unknown READ type: ${ringChannel.type}")
                        }

                        key.isWritable -> when (ringChannel.type) {
                            RingChannel.Type.JOIN_ACCEPT_WRITE -> {
                                if (ring.sendMembershipList(key.attachment() as SendMembershipListChannel)) {
                                    val channel = key.channel() as SocketChannel
                                    logger.debug(tag, "sent membership list, closing JOIN connection to ${channel.remoteAddress}")
                                    channel.close()
                                }
                            }

                            RingChannel.Type.JOIN_CONNECT_WRITE -> {
                                if (ring.sendJoinRequest(key.attachment() as SendActionChannel)) {
                                    val channel = key.channel() as SocketChannel
                                    val readChannel = ReadMembershipListChannel(
                                            RingChannel.Type.JOIN_CONNECT_READ, channel, readableMessageFactory, membershipListMapper, logger
                                    )

                                    key.interestOps(SelectionKey.OP_READ)
                                    key.attach(readChannel)
                                }
                            }

                            RingChannel.Type.PREDECESSOR_CONNECT_WRITE -> {
                                val channel = key.channel() as SocketChannel
                                if (ring.sendIdentity(key.attachment() as SendActionChannel)) {
                                    val readChannel = ReadActionChannel(
                                            RingChannel.Type.PREDECESSOR_ACTION_READ, channel, readableMessageFactory, actionMapper, logger
                                    )

                                    key.interestOps(SelectionKey.OP_READ)
                                    key.attach(readChannel)
                                }
                            }

                            RingChannel.Type.SUCCESSOR_ACTION_WRITE -> {
                                val successor = successorChannels.getValue(key)

                                try {
                                    ring.sendActionsToSuccessor(successor, key.attachment() as BufferedSendActionChannel)
                                } catch (e: IOException) {
                                    logger.warn(tag, "error writing to $successor")
                                }

                                if (shouldILeaveRing) {
                                    key.channel().close()
                                    successorChannels.remove(key)
                                }
                            }

                            else -> throw IllegalArgumentException("unknown WRITE type: ${ringChannel.type}")
                        }

                        else -> throw Throwable("unknown channel and operation!")
                    }

                    // if membership list has changed, rebuild ring
                    if (ring.rebuildRing) {
                        rebuildRing(selector, pipe.sink())
                    }
                }

                if (shouldILeaveRing && successorChannels.isEmpty()) {
                    logger.info(tag, "disconnected from cluster; terminating")
                    break
                }

                if (selector.keys().isEmpty()) {
                    logger.info(tag, "no more open channels, exiting!")
                    break
                }
            }
        }
    }

    // send heartbeat
    private fun startSendingHeartbeats() {
        val currentSuccessors = successorChannels.values.toList()
        heartbeatTimerTask = timer.scheduleAtFixedRate(delay = 0, period = HEARTBEAT_INTERVAL) {
            DatagramSocket().use { socket ->
                currentSuccessors.forEach { successor ->
                    logger.debug(tag, "sending ${heartbeatByteArray.size} heartbeat to ${successor.addr}")
                    socket.send(DatagramPacket(heartbeatByteArray, heartbeatByteArray.size, successor.addr))
                }
            }
        }
    }

    private fun disconnectFromPredecessors() {
        predecessorMonitor.stop()

        val predecessorIterator = predecessorChannels.iterator()
        while (predecessorIterator.hasNext()) {
            val (key, _) = predecessorIterator.next()
            predecessorIterator.remove()
            key.channel().close()
        }
    }

    private fun rebuildRing(
            selector: Selector,
            writableByteChannel: WritableByteChannel
    ) {
        logger.info(tag, "rebuilding ring")

        val currentPredecessors = ring.getPredecessors()
        val currentSuccessors = ring.getSuccessors()

        val connectedPredecessors: Collection<Node> = predecessorChannels.values
        val connectedSuccessors: Collection<Node> = successorChannels.values

        // Identify changes in successor list
        val staleSuccessors = connectedSuccessors.minus(currentSuccessors)
        val newSuccessors = currentSuccessors.minus(connectedSuccessors)

        val successorIterator = successorChannels.iterator()
        while (successorIterator.hasNext()) {
            val (key, node) = successorIterator.next()

            if (node in staleSuccessors) {
                logger.debug(tag, "closing successor connection to ${node.addr}")
                successorIterator.remove()
                key.channel().close()

                ring.removeSuccessor(node)
            }
        }

        if (staleSuccessors.isNotEmpty() || newSuccessors.isNotEmpty()) {
            logger.info(tag, "Stopping successor heartbeat timer")
            heartbeatTimerTask?.cancel()

            if (currentSuccessors.isNotEmpty()) {
                logger.info(tag, "List of successors has changed: $currentSuccessors")
                logger.info(tag, "starting successor heartbeat timer")
                startSendingHeartbeats()
            }
        }

        // Identify changes in predecessor list
        val stalePredecessors = connectedPredecessors.minus(currentPredecessors)
        val newPredecessors = currentPredecessors.minus(connectedPredecessors)

        val predecessorIterator = predecessorChannels.iterator()
        while (predecessorIterator.hasNext()) {
            val (key, node) = predecessorIterator.next()

            if (node in stalePredecessors) {
                logger.debug(tag, "closing predecessor connection to ${node.addr}")
                predecessorIterator.remove()
                key.channel().close()
            }
        }

        for (node in newPredecessors) {
            connectToPredecessor(selector, node)
        }

        if (stalePredecessors.isNotEmpty() || newPredecessors.isNotEmpty()) {
            logger.info(tag, "Stopping predecessor heartbeat monitor")
            predecessorMonitor.stop()

            if (currentPredecessors.isNotEmpty()) {
                logger.info(tag, "List of predecessors has changed: $currentPredecessors")
                logger.info(tag, "starting predecessor heartbeat monitor")
                predecessorMonitor.start(writableByteChannel, currentPredecessors)
            }
        }
    }

    private fun connectToPredecessor(selector: Selector, predecessor: Node) {
        val channel = SocketChannel.open()
        channel.configureBlocking(false)

        val selectionKey = channel.register(selector, SelectionKey.OP_CONNECT, RingChannelImpl(RingChannel.Type.PREDECESSOR_CONNECT))
        predecessorChannels[selectionKey] = predecessor

        logger.info(tag, "connecting to predecessor at ${predecessor.addr}")
        channel.connect(predecessor.addr)
    }
}
