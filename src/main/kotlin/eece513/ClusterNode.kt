package eece513

import eece513.model.MembershipList
import eece513.model.Node
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.channels.*
import java.time.Instant

fun main(args: Array<String>) {
    val logger = TinyLogWrapper()
    val node = ClusterNode(logger)

    if (args.isNotEmpty()) {
        node.join(InetSocketAddress(InetAddress.getByName(args.first()), PORT))
    }

    node.start()
}

class ClusterNode(private val logger: Logger) {
    private val localAddr = InetSocketAddress(InetAddress.getLocalHost(), PORT)

    private var membershipList = MembershipList(emptyList())

    private var predecessors: List<Node> = emptyList()
    private lateinit var predecessorHeartbeatChannel: DatagramChannel
    private lateinit var predecessorServerChannel: ServerSocketChannel
    private var predecessorActionChannels: List<SocketChannel> = emptyList()

    private var successorActionChannels: List<SocketChannel> = emptyList()
    private var successorHeartbeatChannels: List<DatagramChannel> = emptyList()

    private lateinit var self: Node

    fun join(addr: SocketAddress) {
        // is this in blocking mode? I think it is
        // which is ok as we are connecting to the cluster
        SocketChannel
                .open()
                .use { channel ->
                    val connected = channel.connect(addr)

                    if (!connected) throw Throwable("wasn't able to connect!")

                    // Send empty request. The join server will get our url/ip from the request
                    // and send back a membership list
                    channel.write(ByteBuffer.allocate(0))

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

        predecessors = getPredecessors()

        successorActionChannels = buildSuccessorActionChannels()
        successorHeartbeatChannels = buildSuccessorHeartbeatChannels()

        predecessorHeartbeatChannel = DatagramChannel.open().bind(localAddr)
        predecessorHeartbeatChannel.configureBlocking(false)

        predecessorServerChannel = ServerSocketChannel.open().bind(localAddr)
        predecessorServerChannel.configureBlocking(false)

        val selector = Selector.open()

        successorActionChannels.forEach { it.register(selector, SelectionKey.OP_WRITE) }
        successorHeartbeatChannels.forEach { it.register(selector, SelectionKey.OP_WRITE) }
        predecessorHeartbeatChannel.register(selector, SelectionKey.OP_READ)
        predecessorServerChannel.register(selector, SelectionKey.OP_ACCEPT)

        while (true) {
            // block until we have at least one channel ready to use
            logger.debug("tag", "before")
            selector.select()
            logger.debug("tag", "after")

            val selectionKeySet = selector.selectedKeys()
            for (key in selectionKeySet) {
                when {
                    key.isWritable -> {
                        // use the attachment field to distinguish between datagram and socket channels
                        TODO()
                    }

                    key.isReadable -> {
                        val channel = key.channel() as DatagramChannel
                        val remoteAddr = channel.receive(ByteBuffer.allocate(0))

                        logger.debug("tag", "${remoteAddr}")

//                        TODO()
                    }

                    key.isAcceptable -> acceptPredecessorConnection(key.channel() as SocketChannel)
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
        if (membershipList.nodes.isEmpty() || membershipList.nodes.size < 2) return emptyList()

        TODO()
    }

    private fun buildSuccessorActionChannels(): List<SocketChannel> {
        if (membershipList.nodes.isEmpty() || membershipList.nodes.size < 2) return emptyList()

        // put channels in non-blocking mode
        // channel.configureBlocking(false)
        TODO()
    }

    private fun buildSuccessorHeartbeatChannels(): List<DatagramChannel> {
        if (membershipList.nodes.isEmpty() || membershipList.nodes.size < 2) return emptyList()

        // put channels in non-blocking mode
        // channel.configureBlocking(false)
        TODO()
    }

    private fun processPredecessorHeartbeat(channel: SocketChannel) {
        TODO()
    }

    private fun acceptPredecessorConnection(channel: SocketChannel) {
        TODO()
    }
}