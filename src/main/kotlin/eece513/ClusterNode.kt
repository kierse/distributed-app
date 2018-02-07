package eece513

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
//    val person = Person.newBuilder()
//    person.setId(2)
//    person.setName("john")
//    person.setEmail("a@a.com")

    node.start()
}

class ClusterNode(private val logger: Logger) {
    private enum class ServerSocketAttach {
        PREDECESSOR, JOIN
    }

    private val localAddr = InetSocketAddress(InetAddress.getLocalHost(), PORT)

    private var membershipList = MembershipList(emptyList())

    private var predecessors: List<Node> = emptyList()

    private lateinit var predecessorHeartbeatChannel: DatagramChannel
    private lateinit var predecessorServerChannel: ServerSocketChannel
    private var predecessorActionChannels: List<SocketChannel> = emptyList()

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
        if (membershipList.nodes.isEmpty()) {
            println("Cluster join address: ${localAddr.hostName}")
            self = Node(localAddr, Instant.now())
            membershipList = MembershipList(listOf(self))
        } else {
            self = getSelfNode()
        }

        // start listening for incoming join connections
        joinRequestServerChannel = ServerSocketChannel.open().bind(InetSocketAddress(InetAddress.getLocalHost(), JOIN_PORT))
        joinRequestServerChannel.configureBlocking(false)

        startSendingHeartbeats()

        predecessors = getPredecessors()

        successorActionChannels = buildSuccessorActionChannels()

        predecessorHeartbeatChannel = DatagramChannel.open().bind(localAddr)
        predecessorHeartbeatChannel.configureBlocking(false)

        predecessorServerChannel = ServerSocketChannel.open().bind(localAddr)
        predecessorServerChannel.configureBlocking(false)

        val selector = Selector.open()

        successorActionChannels.forEach { it.register(selector, SelectionKey.OP_WRITE) }
        successorHeartbeatChannels.forEach { it.register(selector, SelectionKey.OP_WRITE) }
        predecessorHeartbeatChannel.register(selector, SelectionKey.OP_READ)

        predecessorServerChannel
                .register(selector, SelectionKey.OP_ACCEPT)
                .apply {
                    attach(ServerSocketAttach.PREDECESSOR)
                }

        joinRequestServerChannel
                .register(selector, SelectionKey.OP_ACCEPT)
                .apply {
                    attach(ServerSocketAttach.JOIN)
                }

        while (true) {
            // block until we have at least one channel ready to use
            selector.select()

            val selectionKeySet = selector.selectedKeys()
            for (key in selectionKeySet) {
                when {
                    key.isWritable -> {
                        // use the attachment field to distinguish between datagram and socket channels
                        TODO()
                    }

                    key.isReadable -> {
                        processPredecessorHeartbeat(key.channel() as DatagramChannel)
                        selectionKeySet.remove(key)
                    }

                    key.isAcceptable -> processConnectionAttempt(
                            key.channel() as ServerSocketChannel,
                            key.attachment() as ServerSocketAttach
                    )
                }

                selectionKeySet.remove(key)
            }
        }
    }

    private fun getSelfNode(): Node {
        membershipList.nodes.forEach { node ->
            if (node.addr == localAddr) return node
        }

        throw Throwable("couldn't find matching node for ${localAddr.hostName}")
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

    private fun processConnectionAttempt(channel: ServerSocketChannel, attach: ServerSocketAttach) =
            when (attach) {
                ServerSocketAttach.PREDECESSOR -> processPredecessorConnection(channel)
                ServerSocketAttach.JOIN -> processJoinConnection(channel)
            }

    private fun processPredecessorConnection(channel: ServerSocketChannel) {
        TODO()
    }

    private fun processJoinConnection(channel: ServerSocketChannel) {
        val newNode = channel.accept()
               .use { socketChannel ->
                   Node(socketChannel.remoteAddress, Instant.now())
               }

        val newList = MembershipList(
                membershipList.nodes.plus(newNode)
        )

        logger.debug("tag", "$newList")
    }
}