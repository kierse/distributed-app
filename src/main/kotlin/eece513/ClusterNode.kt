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

                    // get length of membership list (in bytes)
                    val msgLengthBuffer = ByteBuffer.allocate(COMMAND_LENGTH_BUFFER_SIZE)
                    while (msgLengthBuffer.hasRemaining()) {
                        channel.read(msgLengthBuffer)
                    }

                    msgLengthBuffer.flip()
                    val header = msgLengthBuffer.short.toInt()

                    logger.debug(tag, "join response header: $header")

                    val msgBuffer = ByteBuffer.allocate(header)

                    while (msgBuffer.hasRemaining()) {
                        channel.read(msgBuffer)
                    }

                    logger.debug(tag, "read ${msgBuffer.position()} bytes from join server")

                    msgBuffer.flip()
                    membershipList = bytesToMembershipList(msgBuffer)

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
        predecessorNodes = returnThreePredecessors()

        startSendingHeartbeats()

        predecessorHeartbeatChannel = DatagramChannel.open().bind(socketAddr)
        predecessorHeartbeatChannel.configureBlocking(false)

        predecessorServerChannel = ServerSocketChannel.open().bind(socketAddr)
        predecessorServerChannel.configureBlocking(false)

        val selector = Selector.open()

        predecessorHeartbeatChannel
                .register(selector, SelectionKey.OP_READ, ChannelType.PREDECESSOR_HEARTBEAT_READ)

        predecessorServerChannel
                .register(selector, SelectionKey.OP_ACCEPT, ChannelType.PREDECESSOR_ACCEPT)

        // Note: only the first node in a cluster (aka the join server) will listen for join requests
        joinRequestServerChannel
                ?.register(selector, SelectionKey.OP_ACCEPT, ChannelType.JOIN_ACCEPT)

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
                        processPredecessorHeartbeat(key.channel() as DatagramChannel)

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
                    pushMembershipListNewNode(channel)
                }
            }

            // send pending actions to collected predecessors
            for (channel in predecessorChannels) {
                pendingPredecessorActions.remove(channel.remoteAddress)
                        ?.let { list ->
                            pushActionsToPredecessor(channel, list)
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

    private fun readActions(channel: SocketChannel): List<Action> {
        TODO()
    }

    private fun pushActionsToPredecessor(channel: SocketChannel, actions: List<Action>) {
        TODO()
    }

    private fun pushMembershipListNewNode(channel: SocketChannel) {
        val byteArray = membershipListToBytes()
        val count = byteArray.size.toShort()

        // Create message buffer
        // Note: add 2 extra bytes for message header
        val buffer = ByteBuffer.allocate(count + 2)
        buffer.clear()
        buffer.putShort(count)
        buffer.put(byteArray)
        buffer.flip()

        logger.debug(tag, "writing $count bytes to ${channel.remoteAddress} join channel")
        while (buffer.hasRemaining()) {
            channel.write(buffer)
        }
    }

    private fun getSelfNode(): Node {
        membershipList.nodes.forEach { node ->
            if (node.addr == socketAddr) return node
        }

        throw Throwable("couldn't find matching node for ${socketAddr.hostName}")
    }

//    private fun getPredecessors(): List<Node> {
//        if (membershipList.nodes.size < 2) return emptyList()
//
//        //TODO()
//        val path =  "asdf"
//        val inputStream: InputStream = File(path).inputStream()
//        val lineList = mutableListOf<String>()
//
//        inputStream.bufferedReader().useLines { lines -> lines.forEach { lineList.add(it)} }
//
//        val newList = mutableListOf<String>()
//
//        val whatismyip = URL("http://checkip.amazonaws.com")
//        val buffer = BufferedReader(InputStreamReader(
//                whatismyip.openStream()))
//
//        var ip = buffer.readLine() //you get the IP as a String
//        ip = ip.replace(".","-")
//        ip = "ec2-"+ ip + ".ca-central-1.compute.amazonaws.com"
//
//
//        val position = lineList.indexOf(ip)
//        val size = lineList.size
//
//        newList.add(lineList.elementAt(position%size))
//        newList.add(lineList.elementAt((position+1)%size))
//        newList.add(lineList.elementAt((position+2)%size))
//        return emptyList()
//    }

    // connect to successors
    private fun buildSuccessorActionChannels(): List<SocketChannel> {
        if (membershipList.nodes.size < 2) return emptyList()

        // put channels in non-blocking mode
        // channel.configureBlocking(false)
        TODO()
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

//    private fun getPredecessorIndices(): IntArray {
//        val nodes = membershipList.nodes
//
//        val size = nodes.size
//        val indices = intArrayOf(size - 1, size - 2, size - 3)
//
//        for (i in 0 until size) {
//            if (nodes[i] == self) break
//
//            // shuffle everything to the right
//            indices[2] = indices[1]
//            indices[1] = indices[0]
//            indices[0] = i
//        }
//
//        return when (size) {
//            2 -> indices.copyOf(1)
//            3 -> indices.copyOf(2)
//            else -> indices
//        }
//    }
//
//    private fun getSuccessorIndices(): IntArray {
//        val nodes = membershipList.nodes
//
//        val size = nodes.size
//        val indices = intArrayOf(0, 1, 2)
//
//        for (i in (0 until size).reversed()) {
//            if (nodes[i] == self) break
//
//            // shuffle everything to the right
//            indices[2] = indices[1]
//            indices[1] = indices[0]
//            indices[0] = i
//        }
//
//        return when (size) {
//            2 -> indices.copyOf(1)
//            3 -> indices.copyOf(2)
//            else -> indices
//        }
//    }

    // what to do when you get heartbeat
    private fun processPredecessorHeartbeat(channel: DatagramChannel) {
        val remoteAddr = channel.receive(ByteBuffer.allocate(0))
        logger.info(tag, "received packet from $remoteAddr")
    }

    private fun buildJoinAction(addr: SocketAddress): Action.Join = Action.Join(Node(addr, Instant.now()))

    private fun returnThreePredecessors(): List<Node> {
        val nodes = membershipList.nodes
        val position = nodes.indexOf(getSelfNode())
        val size = nodes.size

        val predeccessors = mutableListOf<Node>()
        predeccessors.add(nodes.elementAt(Math.floorMod(position - 1, size)))
        predeccessors.add(nodes.elementAt(Math.floorMod(position - 2, size)))
        predeccessors.add(nodes.elementAt(Math.floorMod(position - 3, size)))

        return when (size) {
            1 -> emptyList()
            2 -> predeccessors.subList(0, 1)
            3 -> predeccessors.subList(0, 2)
            else -> predeccessors
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

    fun sendMessage(channel: SocketChannel, type:Actions.Request.Type, node:Node){
        // set buffer
        val byteBuffer = ByteBuffer.allocate(1024)
        // build proto
        val action = Actions.Request.newBuilder()
                .setType(type)
                .setTimestamp(Instant.now().toEpochMilli())
                .setHostName(node.addr.toString())
                .setPort((node.addr as InetSocketAddress).port)
                .build()
        // put proto into buffer

        byteBuffer.put(action.toByteArray())
        // flip buffer
        byteBuffer.flip()
        // write to socket
        channel.write(byteBuffer)
    }

    fun readMessage(channel:SocketChannel){
        val buf = ByteBuffer.allocate(1024)
        val numBytesRead = channel.read(buf)
        if (numBytesRead == -1) {
            // quit
        }
        buf.flip()

        val parsed = Actions.Request.parseFrom(buf)
        when (parsed.getType()){
            Actions.Request.Type.JOIN ->
                println("joined")
            Actions.Request.Type.REMOVE ->
                println("removed")
            Actions.Request.Type.DROP ->
                println("dropped")
            else ->
                println("else")
        }
    }

}