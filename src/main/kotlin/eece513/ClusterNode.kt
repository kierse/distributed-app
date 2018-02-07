package eece513

import eece513.model.Action
import eece513.model.MembershipList
import eece513.model.Node
import java.net.*
import java.nio.ByteBuffer
import java.nio.channels.*
import java.time.Instant
import java.util.*
import kotlin.concurrent.scheduleAtFixedRate

fun main(args: Array<String>) {
    val logger = TinyLogWrapper()
    val node = ClusterNode(logger)

    if (args.isNotEmpty()) {
        node.join(InetSocketAddress(InetAddress.getByName(args.first()), PORT))
    }

    node.start()
}

class ClusterNode(private val logger: Logger) {
    private enum class ChannelType {
        PREDECESSOR_SERVER, PREDECESSOR_ACTION, PREDECESSOR_HEARTBEAT, SUCCESSOR_HEARTBEAT, SUCCESSOR_ACTION, JOIN_SERVER
    }

    private val localAddr = InetAddress.getLocalHost()
//    private val localAddr = InetAddress.getByName("10.0.0.34")
    private val socketAddr = InetSocketAddress(localAddr, PORT)

    private var membershipList = MembershipList(emptyList())

    private var predecessors: List<Node> = emptyList()

    private lateinit var predecessorHeartbeatChannel: DatagramChannel
    private lateinit var predecessorServerChannel: ServerSocketChannel
    private var predecessorActionChannels: List<SelectionKey> = emptyList()

    private var successorActionChannels: List<SocketChannel> = emptyList()
    private var successorHeartbeatChannels: List<DatagramChannel> = emptyList()

    private lateinit var joinRequestServerChannel: ServerSocketChannel

    private lateinit var self: Node

    private val timer = Timer("successors heartbeat timer", true)
    private lateinit var heartbeatTimerTask: TimerTask

    fun join(addr: SocketAddress) {
        // is this in blocking mode? I think it is
        // which is ok as we are connecting to the cluster
        SocketChannel
                .open()
                .use { channel ->
                    val connected = channel.connect(addr)

                    if (!connected) throw Throwable("wasn't able to connect!")

                    // get length of membership list (in bytes)
                    val msgLengthBuffer = ByteBuffer.allocate(COMMAND_LENGTH_BUFFER_SIZE)
                    while (msgLengthBuffer.hasRemaining()) {
                        channel.read(msgLengthBuffer)
                    }

                    msgLengthBuffer.position(0)
                    val msgBuffer = ByteBuffer.allocate(msgLengthBuffer.short.toInt())

                    while (msgBuffer.hasRemaining()) {
                        channel.read(msgBuffer)
                    }

                    msgBuffer.position(0)
                    membershipList = bytesToMembershipList(msgBuffer)
                }
    }

    private fun bytesToMembershipList(buffer: ByteBuffer): MembershipList {
        // TODO
        return MembershipList(emptyList())
    }

    fun start() {
        println("Join address: ${localAddr.hostName}")

        if (membershipList.nodes.isEmpty()) {
            self = Node(socketAddr, Instant.now())
            membershipList = MembershipList(listOf(self))
        } else {
            self = getSelfNode()
        }

        startSendingHeartbeats()

        predecessors = getPredecessors()

        predecessorHeartbeatChannel = DatagramChannel.open().bind(socketAddr)
        predecessorHeartbeatChannel.configureBlocking(false)

        successorActionChannels = buildSuccessorActionChannels()

        predecessorServerChannel = ServerSocketChannel.open().bind(socketAddr)
        predecessorServerChannel.configureBlocking(false)

        joinRequestServerChannel = ServerSocketChannel.open().bind(InetSocketAddress(localAddr, JOIN_PORT))
        joinRequestServerChannel.configureBlocking(false)

        val selector = Selector.open()

        successorActionChannels.forEach { channel ->
            channel.register(selector, SelectionKey.OP_WRITE)
                    .apply {
                        attach(ChannelType.SUCCESSOR_ACTION)
                    }
        }

        predecessorHeartbeatChannel.register(selector, SelectionKey.OP_READ)
                .apply {
                    attach(ChannelType.PREDECESSOR_HEARTBEAT)
                }

        predecessorServerChannel
                .register(selector, SelectionKey.OP_ACCEPT)
                .apply {
                    attach(ChannelType.PREDECESSOR_SERVER)
                }

        joinRequestServerChannel
                .register(selector, SelectionKey.OP_ACCEPT)
                .apply {
                    attach(ChannelType.JOIN_SERVER)
                }

        while (true) {
            // block until we have at least one channel ready to use
            selector.select()

            val receivedActions = mutableListOf<Action>()
            val successorActionSelectionKeys = mutableListOf<SelectionKey>()
            val predecessorSocketChannels = mutableListOf<SocketChannel>()

            val selectionKeySet = selector.selectedKeys()
            for (key in selectionKeySet) {
                val type = key.attachment() as ChannelType

                when {
                    key.isWritable && type == ChannelType.SUCCESSOR_ACTION ->
                        successorActionSelectionKeys.add(key)

                    key.isReadable && type == ChannelType.PREDECESSOR_HEARTBEAT ->
                        processPredecessorHeartbeat(key.channel() as DatagramChannel)

                    key.isReadable && type == ChannelType.PREDECESSOR_ACTION ->
                        receivedActions.addAll(readActions(key.channel() as SocketChannel))

                    key.isAcceptable && type == ChannelType.PREDECESSOR_SERVER -> {
                        val serverChannel = key.channel() as ServerSocketChannel
                        predecessorSocketChannels.add(serverChannel.accept())
                    }

                    key.isAcceptable && type == ChannelType.JOIN_SERVER ->
                            receivedActions.add(
                                    processJoinAndCreateAction(key.channel() as ServerSocketChannel)
                            )

                    else -> throw Throwable("unknown channel and operation!")
                }

                selectionKeySet.remove(key)

                processActions(receivedActions)

                // can't push actions to successorActionSelectionKeys here. It may not contain ALL successors
                // how do we solve this??
            }
        }
    }

    private fun processActions(actions: List<Action>) {
        TODO()
    }

    private fun findNodeByAddress(addr: SocketAddress): Node? {
        TODO()
    }

    private fun readActions(channel: SocketChannel): List<Action> {
        TODO()
    }

    private fun getSelfNode(): Node {
        membershipList.nodes.forEach { node ->
            if (node.addr == socketAddr) return node
        }

        throw Throwable("couldn't find matching node for ${socketAddr.hostName}")
    }

    private fun getPredecessors(): List<Node> {
        if (membershipList.nodes.size < 2) return emptyList()

        TODO()
    }

    private fun buildSuccessorActionChannels(): List<SocketChannel> {
        if (membershipList.nodes.size < 2) return emptyList()

        // put channels in non-blocking mode
        // channel.configureBlocking(false)
        TODO()
    }

    private fun startSendingHeartbeats() {
        if (membershipList.nodes.size < 2) return

        val nodes = membershipList.nodes
        val successors = mutableListOf<Node>()

        var i = 0
        var found = false

        while (true) {
            if (nodes[i] == self) {
                if (successors.size == 0) {
                    found = true
                    continue
                } else {
                    break
                }
            }

            if (found) successors.add(nodes[i])
            if (successors.size == 3) break
            i = if (nodes.size > i + 1) i + 1 else 0
        }

        heartbeatTimerTask = timer.scheduleAtFixedRate(delay = 0, period = HEARTBEAT_INTERVAL) {
            successors.forEach { successor ->
                DatagramSocket().use { socket ->
                    socket.send(DatagramPacket(byteArrayOf(), 0, successor.addr))
                }
            }
        }
    }

    private fun processPredecessorHeartbeat(channel: DatagramChannel) {
        println("received packet!")
        channel.receive(ByteBuffer.allocate(10))
//        TODO()
    }

//    private fun processConnectionAttempt(channel: ServerSocketChannel, attach: ChannelType) =
//            when (attach) {
//                ChannelType.PREDECESSOR_SERVER -> processPredecessorConnection(channel)
//                ChannelType.JOIN_SERVER -> processJoinConnection(channel)
//            }

    private fun processPredecessorConnection(channel: ServerSocketChannel) {
        TODO()
    }

    private fun processJoinAndCreateAction(channel: ServerSocketChannel): Action.Join {
        val node = channel.accept()
                .use { socketChannel ->
                    Node(socketChannel.remoteAddress, Instant.now())
                }

        return Action.Join(node)
    }
}