package eece513

import eece513.model.Action
import eece513.model.MembershipList
import eece513.model.Node
import java.net.*
import java.nio.ByteBuffer
import java.nio.channels.*
import java.time.Instant
import java.util.*
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.scheduleAtFixedRate

fun main(args: Array<String>) {
    val logger = TinyLogWrapper()
    val node = ClusterNode(logger)

    if (args.isNotEmpty()) {
        node.join(InetSocketAddress(InetAddress.getByName(args.first()), JOIN_PORT))
    }

    node.start()
}

class ClusterNode(private val logger: Logger) {
    private enum class ChannelType {
        JOIN_ACCEPT,
        PREDECESSOR_ACCEPT,
        PREDECESSOR_ACTION_WRITE,
        PREDECESSOR_HEARTBEAT_READ,
        SUCCESSOR_ACTION_READ,
        SUCCESSOR_HEARTBEAT_WRITE
    }

    private val tag = ClusterNode::class.java.simpleName

    private lateinit var socketAddr: InetSocketAddress
    private val localAddr = InetAddress.getLocalHost()
    private var localPort: Int = -1

    private var membershipList = MembershipList(emptyList())
    private lateinit var successorNodes: List<Node>
    private lateinit var predecessorNodes: List<Node>
    private val predecessorNodesRef = AtomicReference<List<Node>>(emptyList())

    private var predecessorActionChannels = mutableListOf<SelectionKey>()
    private lateinit var predecessorHeartbeatChannel: DatagramChannel
    private lateinit var predecessorServerChannel: ServerSocketChannel

    private var joinRequestServerChannel: ServerSocketChannel? = null

    private lateinit var self: Node

    private val timer = Timer("successors heartbeat timer", true)
    private var heartbeatTimerTask: TimerTask? = null

    // what to do when it wants to join
    fun join(addr: SocketAddress) {
        // is this in blocking mode? I think it is
        // which is ok as we are connecting to the cluster
        SocketChannel
                .open()
                .use { channel ->
                    // opens channel
                    val connected = channel.connect(addr)

                    if (!connected) throw Throwable("wasn't able to connect!")

                    localPort = channel.socket().localPort

                    val bytes = readMessage(channel)
                    if (bytes.remaining() == 0) throw Throwable("no response from join server!")

                    membershipList = bytesToMembershipList(bytes)
                    logger.debug(tag, "membership list: $membershipList")
                }
    }

    private fun bytesToMembershipList(buffer: ByteBuffer): MembershipList {
        val parsed = Actions.MembershipList.parseFrom(buffer)

        val nodes = mutableListOf<Node>()
        for (membership in parsed.nodeList) {
            val addr = InetSocketAddress(membership.hostName, membership.port)
            nodes.add(Node(addr, Instant.ofEpochMilli(membership.timestamp)))
        }

        return MembershipList(nodes)
    }

    private fun membershipListToBytes(): ByteArray {
        val message = Actions.MembershipList.newBuilder()

        membershipList.nodes.forEach {
            val member = Actions.Membership
                    .newBuilder()
                    .setHostName((it.addr as InetSocketAddress).hostName)
                    .setPort(it.addr.port)
                    .setTimestamp(it.joinedAt.toEpochMilli())
                    .build()
            message.addNode(member)
        }

        return message.build().toByteArray()
    }

    fun start() {
        println("Join address: ${localAddr.hostName}")

        if (membershipList.nodes.isEmpty()) {
            // This is the join node. Default to using PORT
            localPort = PORT

            socketAddr = InetSocketAddress(localAddr, localPort)
            logger.info(tag, "binding to $socketAddr")

            membershipList = MembershipList(listOf(Node(socketAddr, Instant.now())))

            joinRequestServerChannel = ServerSocketChannel.open().bind(InetSocketAddress(localAddr, JOIN_PORT))
            joinRequestServerChannel?.configureBlocking(false)
            logger.info(tag, "listening for joins on ${joinRequestServerChannel?.socket()?.localSocketAddress}")
        } else {
            socketAddr = InetSocketAddress(localAddr, localPort)
            logger.info(tag, "binding to $socketAddr")
        }

        self = getSelfNode()
        logger.debug(tag, "found self node: $self")

        successorNodes = returnThreeSuccessors()
        predecessorNodesRef.set(returnThreePredecessors())

        predecessorHeartbeatChannel = DatagramChannel.open().bind(socketAddr)
        predecessorHeartbeatChannel.configureBlocking(false)

        predecessorServerChannel = ServerSocketChannel.open().bind(socketAddr)
        predecessorServerChannel.configureBlocking(false)

        val pipe = Pipe.open()
        val sinkChannel = pipe.sink()

        val sourceChannel = pipe.source()
        sourceChannel.configureBlocking(false)

        val selector = Selector.open()

        sourceChannel.register(selector, SelectionKey.OP_READ, ChannelType.PREDECESSOR_HEARTBEAT_READ)

        predecessorServerChannel
                .register(selector, SelectionKey.OP_ACCEPT, ChannelType.PREDECESSOR_ACCEPT)

        // Note: only the first node in a cluster (aka the join server) will listen for join requests
        joinRequestServerChannel
                ?.register(selector, SelectionKey.OP_ACCEPT, ChannelType.JOIN_ACCEPT)

        startSendingHeartbeats()
        startMonitoringHeartbeats(predecessorHeartbeatChannel, sinkChannel)

        /**
         * [Selector] returns channels that are ready for I/O operations. It blocks until at least one (but possibly
         * many) are ready. At any given time, it may return a subset of predecessor action channels. To sidestep the
         * possibility of pushing actions inconsistently to predecessors, they are placed here as they arrive and
         * processed whenever the related channels are ready for I/O operations.
         */
        val pendingPredecessorActions = mutableMapOf<SocketAddress, MutableList<Action>>()
                .withDefault { mutableListOf() }

        while (true) {
            // block until we have at least one channel ready to use
            selector.select()

            val localActions = mutableListOf<Action>()
            val joinChannels = mutableListOf<SocketChannel>()
            val predecessorChannels = mutableListOf<SocketChannel>()

            val selectionKeySet = selector.selectedKeys()
            for (key in selectionKeySet) {
                val type = key.attachment() as ChannelType

                when {
                    key.isWritable && type == ChannelType.PREDECESSOR_ACTION_WRITE ->
                        predecessorChannels.add(key.channel() as SocketChannel)

                    key.isReadable && type == ChannelType.PREDECESSOR_HEARTBEAT_READ ->
                        processMissedPredecessorHeartbeat(key.channel() as Pipe.SourceChannel)

                    key.isReadable && type == ChannelType.SUCCESSOR_ACTION_READ -> {
                        val channel = key.channel() as SocketChannel
                        val actions = readActions(channel)

                        pendingPredecessorActions.getValue(channel.remoteAddress).addAll(actions)
                        localActions.addAll(actions)
                    }

                    key.isAcceptable && type == ChannelType.PREDECESSOR_ACCEPT -> {
                        val serverChannel = key.channel() as ServerSocketChannel
                        val channel = serverChannel.accept()
                        predecessorActionChannels.add(
                                channel.register(selector, SelectionKey.OP_WRITE, ChannelType.PREDECESSOR_ACTION_WRITE)
                        )
                    }

                    key.isAcceptable && type == ChannelType.JOIN_ACCEPT -> {
                        val serverChannel = key.channel() as ServerSocketChannel
                        val channel = serverChannel.accept()
                        val action = buildJoinAction(channel.remoteAddress)

                        logger.info(tag, "received join request from ${channel.remoteAddress}")

                        pendingPredecessorActions.getValue(channel.remoteAddress).add(action)
                        localActions.add(action)

                        joinChannels.add(channel)
                    }


                    else -> throw Throwable("unknown channel and operation!")
                }

                selectionKeySet.remove(key)
            }

            // update local membership list
            processActions(localActions)

            // process joins and send membership list
            for (channel in joinChannels) {
                channel.use {
                    sendMembershipList(channel)
                }
            }

            // send pending actions to collected predecessors
            for (channel in predecessorChannels) {
                pendingPredecessorActions.remove(channel.remoteAddress)
                        ?.let { list ->
                            sendActions(channel, list)
                        }
            }

            val stalePredecessors = rebuildRings()
            pendingPredecessorActions.minusAssign(stalePredecessors.map { it.addr })
        }
    }

    private fun rebuildRings(): List<Node> {
        // Identify changes in successor list
        val currentSuccessors = returnThreeSuccessors()

        val staleSuccessors = successorNodes.minus(currentSuccessors)
        val newSuccessors = currentSuccessors.minus(successorNodes)

        if (staleSuccessors.isNotEmpty() || newSuccessors.isNotEmpty()) {
            successorNodes = currentSuccessors
            restartHeartbeatTimer()
        }

        // Identify changes in predecessor list
        val currentPredecessors = returnThreePredecessors()

        val stalePredecessors = predecessorNodes.minus(currentPredecessors)
        val newPredecessors = currentPredecessors.minus(predecessorNodes)

        if (stalePredecessors.isNotEmpty() || newPredecessors.isNotEmpty()) {
            // TODO reset predecessor heartbeat timeouts!
            predecessorNodes = currentPredecessors
        }

        return stalePredecessors
    }

    private fun processActions(actions: List<Action>) {
        if (actions.isEmpty()) return

        val newNodes = mutableListOf<Node>()
        val staleNodes = mutableListOf<Node>()

        for (action in actions.toSet()) {
            when (action.type) {
                Action.Type.JOIN -> {
                    logger.debug(tag, "adding ${action.node.addr} to membership list")
                    newNodes.add(action.node)
                }

                Action.Type.LEAVE -> {
                    logger.debug(tag, "removing ${action.node.addr} from membership list")
                    staleNodes.add(action.node)
                }

                Action.Type.DROP -> {
                    logger.debug(tag, "dropping ${action.node.addr} from membership list")
                    staleNodes.add(action.node)
                }
            }
        }

        val newList = membershipList
                        .nodes
                        .minus(staleNodes)
                        .plus(newNodes)

        membershipList = membershipList.copy(nodes = newList)
    }


    private fun getSelfNode(): Node {
        membershipList.nodes.forEach { node ->
            if (node.addr == socketAddr) return node
        }

        throw Throwable("couldn't find matching node for ${socketAddr.hostName}")
    }

    private fun restartHeartbeatTimer() {
        heartbeatTimerTask?.cancel()
        startSendingHeartbeats()
    }

    // send heartbeat
    private fun startSendingHeartbeats() {
        if (membershipList.nodes.size < 2) {
            logger.debug(tag, "Nothing to do, membership list is empty!")
            return
        }

        val successors = returnThreeSuccessors()

        heartbeatTimerTask = timer.scheduleAtFixedRate(delay = 0, period = HEARTBEAT_INTERVAL) {
            successors.forEach { successor ->
                DatagramSocket().use { socket ->
                    logger.debug(tag, "sending heartbeat to ${successor.addr}")
                    socket.send(DatagramPacket(byteArrayOf(), 0, successor.addr))
                }
            }
        }
    }

    private fun startMonitoringHeartbeats(heartbeatChannel: DatagramChannel, errorChannel: Pipe.SinkChannel) {

    }

    // what to do when you get heartbeat
    private fun processMissedPredecessorHeartbeat(channel: Pipe.SourceChannel) {
        // TODO
    }

    private fun buildJoinAction(addr: SocketAddress): Action.Join = Action.Join(Node(addr, Instant.now()))

    private fun returnThreePredecessors(): List<Node> {
        val nodes = membershipList.nodes
        val position = nodes.indexOf(getSelfNode())
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
        val position = nodes.indexOf(getSelfNode())
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

    private fun readActions(channel: ReadableByteChannel): List<Action> {
        val actions = mutableListOf<Action>()

        do {
            val bytes = readMessage(channel)
            if (bytes.remaining() == 0) break

            val parsed = Actions.Request.parseFrom(bytes)

            val addr = InetSocketAddress(parsed.hostName, parsed.port)
            val node = Node(addr, Instant.ofEpochMilli(parsed.timestamp))

            val action: Action = when (parsed.type) {
                Actions.Request.Type.JOIN -> Action.Join(node)
                Actions.Request.Type.REMOVE -> Action.Leave(node)
                Actions.Request.Type.DROP -> Action.Drop(node)

                else -> throw IllegalArgumentException("unrecognized type! ${parsed.type.name}")
            }

            actions.add(action)
        } while (true)

        return actions
    }

    private fun sendActions(channel: SocketChannel, actions: List<Action>) {
        for (action in actions) {
            val type = when (action.type) {
                Action.Type.JOIN -> Actions.Request.Type.JOIN
                Action.Type.LEAVE -> Actions.Request.Type.REMOVE
                Action.Type.DROP -> Actions.Request.Type.DROP
            }

            val addr = action.node.addr as InetSocketAddress
            val request = Actions.Request.newBuilder()
                    .setType(type)
                    .setTimestamp(Instant.now().toEpochMilli())
                    .setHostName(addr.hostString)
                    .setPort(addr.port)
                    .build()

            sendMessage(channel, request.toByteArray())
        }
    }

    private fun sendMembershipList(channel: SocketChannel) = sendMessage(channel, membershipListToBytes())

    private fun sendMessage(channel: WritableByteChannel, bytes: ByteArray) {
        val count = bytes.size.toShort()

        // Create message buffer
        // Note: add 2 extra bytes for message header
        val buffer = ByteBuffer.allocate(count + 2)
        buffer.clear()
        buffer.putShort(count)
        buffer.put(bytes)
        buffer.flip()

        while (buffer.hasRemaining()) {
            channel.write(buffer)
        }
        logger.debug(tag, "sendMessage: wrote $count byte(s) to channel")
    }

    private fun readMessage(channel: ReadableByteChannel): ByteBuffer {
        val msgLengthBuffer = ByteBuffer.allocate(MESSAGE_HEADER_SIZE)
        msgLengthBuffer.clear()

        while (msgLengthBuffer.hasRemaining()) {
            channel.read(msgLengthBuffer)
        }

        if (msgLengthBuffer.position() == 0) return ByteBuffer.allocate(0)

        logger.debug(tag, "readMessage: read ${msgLengthBuffer.position()} header byte(s)")

        msgLengthBuffer.flip()
        val header = msgLengthBuffer.short.toInt()

        logger.debug(tag, "readMessage: message body is $header byte(s)")

        val msgBuffer = ByteBuffer.allocate(header)
        while (msgBuffer.hasRemaining()) {
            channel.read(msgBuffer)
        }

        logger.debug(tag, "readMessage: read ${msgBuffer.position()} message body byte(s)")
        return msgBuffer.flip()
    }
}