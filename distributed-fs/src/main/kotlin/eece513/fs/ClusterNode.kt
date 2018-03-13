package eece513.fs

import eece513.common.CONNECTION_PORT
import eece513.common.Logger
import eece513.common.TinyLogWrapper
import eece513.common.mapper.ConnectionPurposeMapper
import eece513.fs.channel.*
import eece513.fs.mapper.*
import eece513.fs.message.ReadableMessageFactory
import eece513.fs.message.SendableMessageFactory
import eece513.common.model.Action
import eece513.common.model.ConnectionPurpose
import eece513.common.model.Node
import eece513.fs.ring.FileSystem
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
//    val logger = TinyLogWrapper(LOG_LOCATION)
    val logger = TinyLogWrapper()

    val localAddr = InetAddress.getLocalHost()
//    val localAddr = InetAddress.getByName("127.0.0.1")
    val socketAddr: InetSocketAddress = ServerSocket(0).use { InetSocketAddress(localAddr, it.localPort) }
    val self = Node(socketAddr, Instant.now())

    val fileSystem = FileSystem(logger)

    val heartbeatCoroutineChannel = Channel<PredecessorHeartbeatMonitorController.Heartbeat>(3)
    val ring = RingImpl(self, fileSystem, heartbeatCoroutineChannel, logger)
    val predecessorMonitor = PredecessorHeartbeatMonitorController(heartbeatCoroutineChannel, SendableMessageFactory(), logger)

    ring.initialize()

    val clusterActionMapper = ClusterActionMapper()
    val connectionPurposeMapper = ConnectionPurposeMapper()
    val membershipListMapper = MembershipListMapper()
    val nodeMapper = NodeMapper()
    val fileSystemMapper = FileSystemMapper()
    val actionByteMapper = ActionMapper(clusterActionMapper, fileSystemMapper)
    val fsCommandMapper = FsCommandObjectMapper()
    val fsResponseMapper = FsResponseByteMapper()

    val sendableMessageFactory = SendableMessageFactory()
    val readableMessageFactory = ReadableMessageFactory()

    val node = ClusterNode(
            localAddr,
            socketAddr,
            self,
            ring,
            predecessorMonitor,
            clusterActionMapper,
            membershipListMapper,
            nodeMapper,
            connectionPurposeMapper,
            fsCommandMapper,
            fsResponseMapper,
            actionByteMapper,
            sendableMessageFactory,
            readableMessageFactory,
            logger
    )

    var address: InetSocketAddress? = null
    var interval = 0L

    if (args.isNotEmpty()) {
        address = InetSocketAddress(InetAddress.getByName(args.first()), CONNECTION_PORT)
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
        private val clusterActionMapper: ClusterActionMapper,
        private val membershipListMapper: MembershipListMapper,
        private val nodeMapper: NodeMapper,
        private val connectionPurposeMapper: ConnectionPurposeMapper,
        private val fsCommandObjectMapper: FsCommandObjectMapper,
        private val fsResponseMapper: FsResponseByteMapper,
        private val actionMapper: ActionMapper,
        private val sendableMessageFactory: SendableMessageFactory,
        private val readableMessageFactory: ReadableMessageFactory,
        private val logger: Logger
) {

    private val tag = ClusterNode::class.java.simpleName

    private val timer = Timer("HEARTBEAT-TIMER", true)
    private var heartbeatTimerTask: TimerTask? = null

    private val successorChannels = mutableMapOf<SelectionKey, Node>()
    private val predecessorChannels = mutableMapOf<SelectionKey, Node>()

    private val heartbeatByteArray = clusterActionMapper.toByteArray(Action.ClusterAction.Heartbeat(self))

    fun start(address: SocketAddress?, interval: Long) {
        println("Join address: ${localAddr.hostName}")

        var terminateAround: Instant? = null
        if (interval > 0L) {
            terminateAround = Instant.now().plusSeconds(interval)
        }

        logger.info(tag, "self node: $self")

        Selector.open().use { selector ->
            logger.info(tag, "listening for TCP connections on $socketAddr")
            val successorServerChannel = ServerSocketChannel.open().bind(socketAddr)
            successorServerChannel.configureBlocking(false)
            successorServerChannel.register(selector, SelectionKey.OP_ACCEPT, RingChannelImpl(RingChannel.Type.SUCCESSOR_ACCEPT))

            logger.info(tag, "listening for UDP connections on $socketAddr")
            val predecessorHeartbeatChannel = DatagramChannel.open().bind(socketAddr)
            val heartbeatChannel = ReadHeartbeatChannel(
                    predecessorHeartbeatChannel, clusterActionMapper, logger
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

            val joinRequestServerChannel = ServerSocketChannel.open().bind(InetSocketAddress(localAddr, CONNECTION_PORT))
            joinRequestServerChannel?.configureBlocking(false)
            joinRequestServerChannel?.register(selector, SelectionKey.OP_ACCEPT, RingChannelImpl(RingChannel.Type.NODE_ACCEPT))
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
                                    val sendChannel = SendConnectionPurposeChannel(
                                            RingChannel.Type.JOIN_CONNECT_PURPOSE_WRITE, channel, sendableMessageFactory, connectionPurposeMapper
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
                                        val sendChannel = SendClusterActionChannel(
                                                RingChannel.Type.PREDECESSOR_CONNECT_WRITE, channel, sendableMessageFactory, clusterActionMapper
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
                            RingChannel.Type.NODE_ACCEPT -> {
                                val serverChannel = key.channel() as ServerSocketChannel
                                val channel = serverChannel.accept()

                                val readConnectionPurposeChannel = ReadConnectionPurposeChannel(
                                        RingChannel.Type.NODE_ACCEPT_READ, channel, readableMessageFactory, connectionPurposeMapper, logger
                                )

                                channel.configureBlocking(false)
                                channel.register(selector, SelectionKey.OP_READ, readConnectionPurposeChannel)
                                logger.debug(tag, "new incoming connection from ${channel.remoteAddress}")
                            }

                            RingChannel.Type.SUCCESSOR_ACCEPT -> {
                                val serverChannel = key.channel() as ServerSocketChannel
                                val channel = serverChannel.accept()

                                val readChannel = ReadClusterActionChannel(
                                        RingChannel.Type.SUCCESSOR_ACCEPT_READ, channel, readableMessageFactory, clusterActionMapper, logger
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

                            RingChannel.Type.NODE_ACCEPT_READ -> {
                                val socketChannel = key.attachment() as ReadConnectionPurposeChannel
                                val purpose = socketChannel.read()

                                val channel = key.channel() as SocketChannel
                                when (purpose) {
                                    is ConnectionPurpose.NodeJoin -> {
                                        val readActionChannel = ReadClusterActionChannel(
                                                RingChannel.Type.JOIN_ACCEPT_READ, channel, readableMessageFactory, clusterActionMapper, logger
                                        )

                                        key.interestOps(SelectionKey.OP_READ)
                                        key.attach(readActionChannel)
                                        logger.debug(tag, "accepting connection from new node at ${channel.remoteAddress}")
                                    }

                                    is ConnectionPurpose.ClientRemove -> {
                                        logger.debug(tag, "identified ClientRemove purpose")
                                        val readCommandChannel = ReadFsCommandChannel(
                                                RingChannel.Type.CLIENT_REMOVE, channel, readableMessageFactory, fsCommandObjectMapper, logger
                                        )

                                        key.interestOps(SelectionKey.OP_READ)
                                        key.attach(readCommandChannel)
                                    }

                                    is ConnectionPurpose.ClientGet -> {
                                        logger.debug(tag, "identified ClientGet purpose")
                                        val readCommandChannel = ReadFsCommandChannel(
                                                RingChannel.Type.CLIENT_GET, channel, readableMessageFactory, fsCommandObjectMapper, logger
                                        )

                                        key.interestOps(SelectionKey.OP_READ)
                                        key.attach(readCommandChannel)
                                    }

                                    is ConnectionPurpose.ClientPut -> {
                                        logger.debug(tag, "identified ClientPut purpose")
                                        val readCommandChannel = ReadFsCommandChannel(
                                                RingChannel.Type.CLIENT_PUT, channel, readableMessageFactory, fsCommandObjectMapper, logger
                                        )

                                        key.interestOps(SelectionKey.OP_READ)
                                        key.attach(readCommandChannel)
                                    }
                                }
                            }

                            RingChannel.Type.CLIENT_REMOVE -> {
                                if (ring.processFileRemove(key.attachment() as ReadFsCommandChannel)) {
                                    val channel = key.channel() as SocketChannel
                                    val sendChannel = SendFsResponseChannel(
                                            RingChannel.Type.CLIENT_REMOVE_WRITE, channel, sendableMessageFactory, fsResponseMapper
                                    )

                                    key.interestOps(SelectionKey.OP_WRITE)
                                    key.attach(sendChannel)
                                }
                            }

                            RingChannel.Type.CLIENT_GET -> {
                                val response = ring.processFileGet(key.attachment() as ReadFsCommandChannel)
                                if (response != null) {
                                    val channel = key.channel() as SocketChannel
                                    val sendChannel = SendFsResponseChannel(
                                            RingChannel.Type.CLIENT_GET_WRITE, channel, sendableMessageFactory, fsResponseMapper
                                    )

                                    val stateChannel = FsResponseStateChannel(
                                            RingChannel.Type.CLIENT_GET_WRITE, response, sendChannel
                                    )

                                    key.interestOps(SelectionKey.OP_WRITE)
                                    key.attach(stateChannel)
                                }
                            }

                            RingChannel.Type.CLIENT_PUT -> {
                                val response = ring.processFilePut(key.attachment() as ReadFsCommandChannel)
                                if (response != null) {
                                    val channel = key.channel() as SocketChannel
                                    val sendChannel = SendFsResponseChannel(
                                            RingChannel.Type.CLIENT_PUT_WRITE, channel, sendableMessageFactory, fsResponseMapper
                                    )

                                    val stateChannel = FsResponseStateChannel(
                                            RingChannel.Type.CLIENT_PUT_WRITE, response, sendChannel
                                    )

                                    key.interestOps(SelectionKey.OP_WRITE)
                                    key.attach(stateChannel)
                                }
                            }

                            RingChannel.Type.CLIENT_PUT_CONFIRM -> {
                                if (ring.processFilePutConfirm(key.attachment() as ReadFsCommandChannel)) {
                                    val channel = key.channel() as SocketChannel
                                    channel.close()
                                }
                            }

                            RingChannel.Type.JOIN_ACCEPT_READ -> {
                                if (ring.processJoinRequest(key.attachment() as ReadClusterActionChannel)) {
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
                                val successor = ring.readIdentity(key.attachment() as ReadClusterActionChannel)
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

                            RingChannel.Type.JOIN_CONNECT_PURPOSE_WRITE -> {
                                val sendPurposeChannel = key.attachment() as SendConnectionPurposeChannel
                                if (sendPurposeChannel.send(ConnectionPurpose.NodeJoin())) {
                                    val channel = key.channel() as SocketChannel
                                    val sendChannel = SendClusterActionChannel(
                                            RingChannel.Type.JOIN_CONNECT_ACTION_WRITE, channel, sendableMessageFactory, clusterActionMapper
                                    )

                                    key.interestOps(SelectionKey.OP_WRITE)
                                    key.attach(sendChannel)
                                }
                            }

                            RingChannel.Type.JOIN_CONNECT_ACTION_WRITE -> {
                                if (ring.sendJoinRequest(key.attachment() as SendClusterActionChannel)) {
                                    val channel = key.channel() as SocketChannel
                                    val readChannel = ReadMembershipListChannel(
                                            RingChannel.Type.JOIN_CONNECT_READ, channel, readableMessageFactory, membershipListMapper, logger
                                    )

                                    key.interestOps(SelectionKey.OP_READ)
                                    key.attach(readChannel)
                                }
                            }

                            RingChannel.Type.CLIENT_REMOVE_WRITE -> {
                                if (ring.sendFileRemoveResponse(key.attachment() as SendFsResponseChannel)) {
                                    val channel = key.channel() as SocketChannel
                                    channel.close()
                                }
                            }

                            RingChannel.Type.CLIENT_GET_WRITE -> {
                                if (ring.sendFileGetResponse(key.attachment() as FsResponseStateChannel)) {
                                    val channel = key.channel() as SocketChannel
                                    channel.close()
                                }
                            }

                            RingChannel.Type.CLIENT_PUT_WRITE -> {
                                val sendChannel = key.attachment() as FsResponseStateChannel
                                if (ring.sendFilePutResponse(sendChannel)) {
                                    logger.info(tag, "send ${sendChannel.response}")
                                    val channel = key.channel() as SocketChannel
                                    val readCommandChannel = ReadFsCommandChannel(
                                            RingChannel.Type.CLIENT_PUT_CONFIRM, channel, readableMessageFactory, fsCommandObjectMapper, logger
                                    )

                                    key.interestOps(SelectionKey.OP_READ)
                                    key.attach(readCommandChannel)
                                }
                            }


                            RingChannel.Type.PREDECESSOR_CONNECT_WRITE -> {
                                val channel = key.channel() as SocketChannel
                                if (ring.sendIdentity(key.attachment() as SendClusterActionChannel)) {
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
