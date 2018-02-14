package eece513

import eece513.model.Action
import eece513.model.MembershipList
import eece513.model.Node
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.runBlocking
import java.io.IOException
import java.net.*
import java.nio.ByteBuffer
import java.nio.channels.*
import java.time.Instant
import java.util.*
import kotlin.concurrent.scheduleAtFixedRate

fun main(args: Array<String>) {
    val logger = TinyLogWrapper()
    val messageReader = MessageReader(logger)
    val messageBuilder = MessageBuilder()
    val actionFactory = ActionFactory(messageReader)
    val membershipListFactory = MembershipListFactory(messageReader)
    val predecessorMonitor = PredecessorHeartbeatMonitorController(messageBuilder, logger)

    val node = ClusterNode2(
            predecessorMonitor, messageBuilder, actionFactory, membershipListFactory, logger
    )
    val address = if (args.isNotEmpty()) {
        InetSocketAddress(InetAddress.getByName(args.first()), JOIN_PORT)
    } else {
        null
    }

    node.start(address)
}

class ClusterNode2(
        private val predecessorMonitor: PredecessorHeartbeatMonitorController,
        private val messageBuilder: MessageBuilder,
        private val actionFactory: ActionFactory,
        private val membershipListFactory: MembershipListFactory,
        private val logger: Logger
) {
    private enum class ChannelType {
        JOIN_ACCEPT,
        JOIN_ACCEPT_READ,
        JOIN_ACCEPT_WRITE,
        JOIN_CONNECT,
        JOIN_CONNECT_WRITE,
        JOIN_CONNECT_READ,
        PREDECESSOR_ACCEPT,
        PREDECESSOR_ACTION_READ,
        PREDECESSOR_ACTION_WRITE,
        PREDECESSOR_CONNECT_WRITE,
        PREDECESSOR_CONNECT,
        PREDECESSOR_HEARTBEAT_READ,
        PREDECESSOR_MISSED_HEARTBEAT_READ,
        SUCCESSOR_ACCEPT,
        SUCCESSOR_ACCEPT_READ,
        SUCCESSOR_ACTION,
        SUCCESSOR_ACTION_READ,
        SUCCESSOR_ACTION_WRITE,
        SUCCESSOR_HEARTBEAT_WRITE
    }

    private val tag = ClusterNode2::class.java.simpleName

    private lateinit var socketAddr: InetSocketAddress
    private val localAddr = InetAddress.getLocalHost()
//    private val localAddr = InetAddress.getByName("127.0.0.1")

    private var membershipList = MembershipList(emptyList())

    private val timer = Timer("HEARTBEAT-TIMER", true)
    private var heartbeatTimerTask: TimerTask? = null

    private val successorChannels = mutableMapOf<SelectionKey, Node>()
    private val predecessorChannels = mutableMapOf<SelectionKey, Node>()

    private val self: Node by lazy {
        membershipList
                .nodes
                .first { it.addr == socketAddr }
                .also {
                    logger.debug(tag, "self node: $it")
                }
    }

    private val heartbeatByteArray: ByteArray by lazy {
        buildHeartbeat(self)
                .toByteArray()
                .also {
                    logger.debug(tag, "heartbeat is ${it.size} byte(s) long")
                }
    }

    fun start(address: SocketAddress?) = runBlocking {
        socketAddr = ServerSocket(0).use { InetSocketAddress(localAddr, it.localPort) }

        if (address == null) {
            println("Join address: ${localAddr.hostName}")
            membershipList = MembershipList(listOf(Node(socketAddr, Instant.now())))
        }

        Selector.open().use { selector ->
            logger.info(tag, "listening for TCP connections on $socketAddr")
            val successorServerChannel = ServerSocketChannel.open().bind(socketAddr)
            successorServerChannel.configureBlocking(false)
            successorServerChannel.register(selector, SelectionKey.OP_ACCEPT, ChannelType.SUCCESSOR_ACCEPT)

            logger.info(tag, "listening for UDP connections on $socketAddr")
            val predecessorHeartbeatChannel = DatagramChannel.open().bind(socketAddr)
            predecessorHeartbeatChannel.configureBlocking(false)
            predecessorHeartbeatChannel.register(selector, SelectionKey.OP_READ, ChannelType.PREDECESSOR_HEARTBEAT_READ)

            val pipe = Pipe.open()
            val heartbeatCoroutineChannel = Channel<PredecessorHeartbeatMonitorController.Heartbeat>(3)

            val predecessorMissedHeartbeatChannel = pipe.source()
            predecessorMissedHeartbeatChannel.configureBlocking(false)
            predecessorMissedHeartbeatChannel.register(selector, SelectionKey.OP_READ, ChannelType.PREDECESSOR_MISSED_HEARTBEAT_READ)

            if (address == null) {
                val joinRequestServerChannel = ServerSocketChannel.open().bind(InetSocketAddress(localAddr, JOIN_PORT))
                joinRequestServerChannel?.configureBlocking(false)
                joinRequestServerChannel?.register(selector, SelectionKey.OP_ACCEPT, ChannelType.JOIN_ACCEPT)
                logger.info(tag, "listening for joins on ${joinRequestServerChannel.socket().localSocketAddress}")
            } else {
                val joinClusterChannel = SocketChannel.open()
                joinClusterChannel.configureBlocking(false)
                joinClusterChannel.register(selector, SelectionKey.OP_CONNECT, ChannelType.JOIN_CONNECT)
                joinClusterChannel.connect(address)
                logger.debug(tag, "connecting to join server!")
            }

            val pendingSuccessorActions = mutableMapOf<Node, MutableList<Action>>()
                    .withDefault { mutableListOf() }

            while (selector.select() >= 0) {
                val keyIterator = selector.selectedKeys().iterator()
                while (keyIterator.hasNext()) {
                    val key = keyIterator.next()
                    keyIterator.remove()

                    // While processing a previous key, it is possible the channel associated with this key was closed
                    // Verify that the key is still valid before attempting to use it
                    if (!key.isValid) continue

                    val type = key.attachment() as ChannelType
                    val currentMembershipList = membershipList

                    when {
                        key.isConnectable -> when (type) {
                            ChannelType.JOIN_CONNECT -> {
                                val channel = key.channel() as SocketChannel
                                if (channel.finishConnect()) {
                                    key.interestOps(SelectionKey.OP_WRITE)
                                    key.attach(ChannelType.JOIN_CONNECT_WRITE)

                                    logger.debug(tag, "completed connection to join server!")
                                }
                            }

                            ChannelType.PREDECESSOR_CONNECT -> {
                                val channel = key.channel() as SocketChannel
                                val predecessor = predecessorChannels[key]
                                        ?: throw IllegalStateException("unable to locate predecessor node")
                                try {
                                    if (channel.finishConnect()) {
                                        key.interestOps(SelectionKey.OP_WRITE)
                                        key.attach(ChannelType.PREDECESSOR_CONNECT_WRITE)

                                        logger.debug(tag, "completed connection to predecessor at ${predecessor.addr}")
                                    }
                                } catch (_: IOException) {
                                    logger.warn(tag, "unable to establish connection to predecessor at ${predecessor.addr}. Dropping!")
                                    val dropAction = Action.Drop(predecessor)

                                    processAction(dropAction)
                                    pendingSuccessorActions.values.forEach { it.add(dropAction) }
                                }
                            }

                            else -> throw IllegalArgumentException("unknown CONNECT type: $type")
                        }

                        key.isAcceptable -> when (type) {
                            ChannelType.JOIN_ACCEPT -> {
                                val serverChannel = key.channel() as ServerSocketChannel
                                val channel = serverChannel.accept()
                                channel.configureBlocking(false)
                                channel.register(selector, SelectionKey.OP_READ, ChannelType.JOIN_ACCEPT_READ)
                                logger.debug(tag, "accepting connection from ${channel.remoteAddress}")
                            }

                            ChannelType.SUCCESSOR_ACCEPT -> {
                                val serverChannel = key.channel() as ServerSocketChannel
                                val channel = serverChannel.accept()
                                channel.configureBlocking(false)
                                channel.register(selector, SelectionKey.OP_READ, ChannelType.SUCCESSOR_ACCEPT_READ)
                                logger.debug(tag, "accepting connection from successor ${channel.remoteAddress}")
                            }

                            else -> throw IllegalArgumentException("unknown ACCEPT type: $type")
                        }

                        key.isReadable -> when (type) {
                            ChannelType.PREDECESSOR_MISSED_HEARTBEAT_READ -> {
                                val channel = key.channel() as ReadableByteChannel
                                val dropAction = actionFactory.build(channel)
                                        ?: throw IllegalArgumentException("unable to build DROP action")

                                logger.info(tag, "received drop command for ${dropAction.node.addr}")
                                processAction(dropAction)

                                pendingSuccessorActions.values.forEach { it.add(dropAction) }
                            }

                            ChannelType.JOIN_ACCEPT_READ -> {
                                val channel = key.channel() as SocketChannel
                                val action = actionFactory.build(channel)
                                        ?: throw IllegalArgumentException("joining node did not send JOIN action")

                                logger.info(tag, "received join request from ${action.node.addr}")
                                processAction(action)

                                key.interestOps(SelectionKey.OP_WRITE)
                                key.attach(ChannelType.JOIN_ACCEPT_WRITE)
                            }


                            ChannelType.JOIN_CONNECT_READ -> {
                                val channel = key.channel() as SocketChannel
                                membershipList = membershipListFactory.build(channel)
                                        ?: throw IllegalArgumentException("join node did not send MEMBERSHIP")
                                logger.info(tag, "received membership list: $membershipList")

                                logger.debug(tag, "closing JOIN connection to ${channel.remoteAddress}")
                                key.cancel()
                            }

                            ChannelType.SUCCESSOR_ACCEPT_READ -> {
                                // connect to new successors
                                val channel = key.channel() as SocketChannel
                                val action = actionFactory.build(channel)
                                        ?: throw IllegalArgumentException("successor did not send CONNECT action")

                                logger.debug(tag, "successor identified as ${action.node.addr}")
                                successorChannels[key] = action.node

                                heartbeatTimerTask?.cancel()
                                startSendingHeartbeats()

                                key.interestOps(SelectionKey.OP_WRITE)
                                key.attach(ChannelType.SUCCESSOR_ACTION_WRITE)
                            }

                            ChannelType.PREDECESSOR_ACTION_READ -> {
                                val channel = key.channel() as SocketChannel
                                val newActions = actionFactory.buildList(channel)

                                if (newActions.isNotEmpty()) {
                                    logger.debug(tag, "found ${newActions.size} actions")

                                    newActions.forEach { processAction(it) }

                                    pendingSuccessorActions.values.forEach { actions ->
                                        actions.addAll(newActions)
                                    }
                                }
                            }

                            ChannelType.PREDECESSOR_HEARTBEAT_READ -> {
                                val channel = key.channel() as DatagramChannel
                                val action = actionFactory.build(channel)
                                        ?: throw IllegalArgumentException("successor did not send HEARTBEAT action")
                                logger.debug(tag, "identified heartbeat from ${action.node.addr}")

                                heartbeatCoroutineChannel.send(
                                        PredecessorHeartbeatMonitorController.Heartbeat(action.node)
                                )
                            }

                            else -> throw IllegalArgumentException("unknown READ type: $type")
                        }

                        key.isWritable -> when (type) {
                            ChannelType.JOIN_ACCEPT_WRITE -> {
                                val channel = key.channel() as SocketChannel
                                logger.debug(tag, "sending membership list to ${channel.remoteAddress}")
                                sendMembershipList(channel)

                                logger.debug(tag, "closing JOIN connection to ${channel.remoteAddress}")
                                key.cancel()
                            }

                            ChannelType.JOIN_CONNECT_WRITE -> {
                                val channel = key.channel() as SocketChannel

                                logger.debug(tag, "sending join request to ${channel.remoteAddress}")
                                sendAction(channel, Action.Join(Node(socketAddr, Instant.now())))

                                key.interestOps(SelectionKey.OP_READ)
                                key.attach(ChannelType.JOIN_CONNECT_READ)
                            }

                            ChannelType.PREDECESSOR_CONNECT_WRITE -> {
                                val channel = key.channel() as SocketChannel

                                logger.debug(tag, "identifying myself as ${self.addr}")
                                sendAction(channel, Action.Connect(self))

                                key.interestOps(SelectionKey.OP_READ)
                                key.attach(ChannelType.PREDECESSOR_ACTION_READ)
                            }

                            ChannelType.SUCCESSOR_ACTION_WRITE -> {
                                val node = successorChannels[key]
                                        ?: throw IllegalStateException("unable to identify channel node!")

                                val channel = key.channel() as SocketChannel
                                pendingSuccessorActions.remove(node)
                                        ?.let { actions ->
                                            sendActions(channel, actions)
                                        }
                            }

                            else -> throw IllegalArgumentException("unknown WRITE type: $type")
                        }

                        else -> throw Throwable("unknown channel and operation!")
                    }

                    // if membership list has changed, rebuild ring
                    if (currentMembershipList != membershipList) {
                        rebuildRing(selector, pipe.sink(), heartbeatCoroutineChannel)
                    }
                }
            }
        }
    }

    private fun restartHeartbeatTimer() {
        heartbeatTimerTask?.cancel()
        startSendingHeartbeats()
    }

    // send heartbeat
    private fun startSendingHeartbeats() {
        val currentSuccessors = successorChannels.values.toList()
        heartbeatTimerTask = timer.scheduleAtFixedRate(delay = 0, period = HEARTBEAT_INTERVAL) {
                    currentSuccessors.forEach { successor ->
                        DatagramSocket().use { socket ->
                            logger.debug(tag, "sending ${heartbeatByteArray.size} byte heartbeat to ${successor.addr}")
                            socket.send(DatagramPacket(heartbeatByteArray, heartbeatByteArray.size, successor.addr))
                        }
                    }
        }
    }

    private fun processAction(action: Action) {
        val nodes = when (action.type) {
            Action.Type.JOIN -> {
                logger.debug(tag, "adding ${action.node.addr} to membership list")
                membershipList.nodes.plus(action.node)
            }

            Action.Type.LEAVE -> {
                logger.debug(tag, "removing ${action.node.addr} from membership list")
                membershipList.nodes.filter { it != action.node }
            }

            Action.Type.DROP -> {
                logger.debug(tag, "dropping ${action.node.addr} from membership list")
                membershipList.nodes.filter { it != action.node }
            }

            Action.Type.HEARTBEAT -> throw IllegalStateException("HEARTBEAT actions should not be handled here!")
            Action.Type.CONNECT -> throw IllegalStateException("CONNECT actions should not be handled here!")
        }

        membershipList = MembershipList(nodes)
        logger.info(tag, "new membership list: $membershipList")
    }

    private fun sendActions(channel: SocketChannel, actions: List<Action>) {
        for (action in actions) {
            val type = when (action.type) {
                Action.Type.JOIN -> Actions.Request.Type.JOIN
                Action.Type.LEAVE -> Actions.Request.Type.REMOVE
                Action.Type.DROP -> Actions.Request.Type.DROP
                Action.Type.CONNECT -> Actions.Request.Type.CONNECT
                Action.Type.HEARTBEAT -> Actions.Request.Type.HEARTBEAT
            }

            val now = Instant.now()
            val timestamp = Actions.Timestamp.newBuilder()
                    .setSecondsSinceEpoch(now.epochSecond)
                    .setNanoSeconds(now.nano)
                    .build()

            val addr = action.node.addr
            val request = Actions.Request.newBuilder()
                    .setType(type)
                    .setTimestamp(timestamp)
                    .setHostName(addr.hostString)
                    .setPort(addr.port)
                    .build()

            sendMessage(channel, messageBuilder.build(request.toByteArray()))
        }
    }

    private fun sendAction(channel: SocketChannel, action: Action) {
        sendActions(channel, listOf(action))
    }

    private fun sendMembershipList(channel: SocketChannel) {
        val builder = Actions.MembershipList.newBuilder()

        membershipList.nodes.forEach {
            val timestamp = Actions.Timestamp.newBuilder()
                    .setSecondsSinceEpoch(it.joinedAt.epochSecond)
                    .setNanoSeconds(it.joinedAt.nano)
                    .build()

            val member = Actions.Membership
                    .newBuilder()
                    .setHostName(it.addr.hostName)
                    .setPort(it.addr.port)
                    .setTimestamp(timestamp)
                    .build()
            builder.addNode(member)
        }

        val message = builder.build()
        sendMessage(channel, messageBuilder.build(message.toByteArray()))
    }

    private fun sendMessage(channel: WritableByteChannel, buffer: ByteBuffer) {
        if (buffer.position() > 0) buffer.flip()

        while (buffer.hasRemaining()) {
            channel.write(buffer)
        }
        logger.debug(tag, "sendMessage: wrote ${buffer.position()} byte(s) to channel")
    }

    private fun rebuildRing(
            selector: Selector,
            writableByteChannel: WritableByteChannel,
            heartbeatChannel: ReceiveChannel<PredecessorHeartbeatMonitorController.Heartbeat>
    ) {
        logger.info(tag, "rebuilding ring")

        val currentPredecessors = returnThreePredecessors()
        val currentSuccessors = returnThreeSuccessors()

        val connectedPredecessor = predecessorChannels.values
        val connectedSuccessors = successorChannels.values

        // Identify changes in successor list
        val staleSuccessors = connectedSuccessors.minus(currentSuccessors)
        val newSuccessors = currentSuccessors.minus(connectedSuccessors)

        // eliminate stale successors
        for ((key, node) in successorChannels) {
            if (node in staleSuccessors) {
                val type = key.attachment() as ChannelType
                logger.debug(tag, "closing connection of type ${type.name}")
                successorChannels.remove(key)
                key.cancel()
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
        val stalePredecessors = connectedPredecessor.minus(currentPredecessors)
        val newPredecessors = currentPredecessors.minus(connectedPredecessor)

        for ((key, node) in predecessorChannels) {
            if (node in stalePredecessors) {
                val type = key.attachment() as ChannelType
                logger.debug(tag, "closing connection of type ${type.name}")
                predecessorChannels.remove(key)
                key.cancel()
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
                predecessorMonitor.start(writableByteChannel, heartbeatChannel, currentPredecessors)
            }
        }
    }

    private fun connectToPredecessor(selector: Selector, predecessor: Node) {
        val channel = SocketChannel.open()
        channel.configureBlocking(false)

        val selectionKey = channel.register(selector, SelectionKey.OP_CONNECT, ChannelType.PREDECESSOR_CONNECT)
        predecessorChannels[selectionKey] = predecessor

        logger.info(tag, "connecting to predecessor at ${predecessor.addr}")
        channel.connect(predecessor.addr)
    }

    private fun returnThreePredecessors(): List<Node> {
        val nodes = membershipList.nodes
        val position = nodes.indexOf(self)
        val size = nodes.size

        val predecessors = mutableListOf<Node>()
        predecessors.add(nodes.elementAt(Math.floorMod(position - 1, size)))
        predecessors.add(nodes.elementAt(Math.floorMod(position - 2, size)))
        predecessors.add(nodes.elementAt(Math.floorMod(position - 3, size)))

        return when (size) {
            1 -> emptyList()
            2 -> predecessors.subList(0, 1)
            3 -> predecessors.subList(0, 2)
            else -> predecessors
        }
    }

    private fun returnThreeSuccessors(): List<Node> {
        val nodes = membershipList.nodes
        val position = nodes.indexOf(self)
        val size = nodes.size

        val successors = mutableListOf<Node>()
        successors.add(nodes.elementAt(Math.floorMod(position + 1, size)))
        successors.add(nodes.elementAt(Math.floorMod(position + 2, size)))
        successors.add(nodes.elementAt(Math.floorMod(position + 3, size)))

        return when (size) {
            1 -> emptyList()
            2 -> successors.subList(0, 1)
            3 -> successors.subList(0, 2)
            else -> successors
        }
    }

    private fun buildHeartbeat(self: Node): Actions.Request {
        val timestamp = Actions.Timestamp.newBuilder()
                .setSecondsSinceEpoch(self.joinedAt.epochSecond)
                .setNanoSeconds(self.joinedAt.nano)
                .build()

        return Actions.Request.newBuilder()
                .setType(Actions.Request.Type.HEARTBEAT)
                .setHostName(self.addr.hostString)
                .setPort(self.addr.port)
                .setTimestamp(timestamp)
                .build()
    }
}
