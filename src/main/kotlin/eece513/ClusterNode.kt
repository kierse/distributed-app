package eece513

import eece513.model.Action
import eece513.model.MembershipList
import eece513.model.Node
import java.io.BufferedReader
import java.io.File
import java.io.InputStream
import java.io.InputStreamReader
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
        JOIN_ACCEPT,
        PREDECESSOR_ACCEPT,
        PREDECESSOR_ACTION_WRITE,
        PREDECESSOR_HEARTBEAT_READ,
        SUCCESSOR_ACTION_READ,
        SUCCESSOR_HEARTBEAT_WRITE
    }

    private val localAddr = InetAddress.getLocalHost()
//    private val localAddr = InetAddress.getByName("10.0.0.34")
    private val socketAddr = InetSocketAddress(localAddr, PORT)

    private var membershipList = MembershipList(emptyList())

    private var predecessorActionChannels = mutableListOf<SelectionKey>()
    private lateinit var predecessorHeartbeatChannel: DatagramChannel
    private lateinit var predecessorServerChannel: ServerSocketChannel

    private lateinit var joinRequestServerChannel: ServerSocketChannel

    private lateinit var self: Node

    private val timer = Timer("successors heartbeat timer", true)
    private lateinit var heartbeatTimerTask: TimerTask

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

        val buf = ByteBuffer.allocate(buffer.capacity())
        buf.flip()

        val parsed = Actions.MembershipList.parseFrom(buf)
        println(parsed)
        return MembershipList(emptyList())
    }

    private fun membershipListToBytes(list: MembershipList): ByteArray {
        // TODO
        val message = Actions.MembershipList.newBuilder()
        list.nodes.forEach{
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
            self = Node(socketAddr, Instant.now())
            membershipList = MembershipList(listOf(self))
        } else {
            self = getSelfNode()
        }

        startSendingHeartbeats()

        predecessorHeartbeatChannel = DatagramChannel.open().bind(socketAddr)
        predecessorHeartbeatChannel.configureBlocking(false)

        predecessorServerChannel = ServerSocketChannel.open().bind(socketAddr)
        predecessorServerChannel.configureBlocking(false)

        joinRequestServerChannel = ServerSocketChannel.open().bind(InetSocketAddress(localAddr, JOIN_PORT))
        joinRequestServerChannel.configureBlocking(false)

        val selector = Selector.open()

        predecessorHeartbeatChannel
                .register(selector, SelectionKey.OP_READ, ChannelType.PREDECESSOR_HEARTBEAT_READ)

        predecessorServerChannel
                .register(selector, SelectionKey.OP_ACCEPT, ChannelType.PREDECESSOR_ACCEPT)

        joinRequestServerChannel
                .register(selector, SelectionKey.OP_ACCEPT, ChannelType.JOIN_ACCEPT)

        val pendingPredecessorActions = mutableMapOf<SocketAddress, MutableList<Action>>()

        while (true) {
            // block until we have at least one channel ready to use
            selector.select()

            val actions = mutableListOf<Action>()
            val predecessorChannels = mutableListOf<SocketChannel>()

            val selectionKeySet = selector.selectedKeys()
            for (key in selectionKeySet) {
                val type = key.attachment() as ChannelType

                when {
                    key.isWritable && type == ChannelType.PREDECESSOR_ACTION_WRITE ->
                        predecessorChannels.add(key.channel() as SocketChannel)

                    key.isReadable && type == ChannelType.PREDECESSOR_HEARTBEAT_READ ->
                        processPredecessorHeartbeat(key.channel() as DatagramChannel)

                    key.isReadable && type == ChannelType.SUCCESSOR_ACTION_READ ->
                        actions.addAll(readActions(key.channel() as SocketChannel))

                    key.isAcceptable && type == ChannelType.PREDECESSOR_ACCEPT -> {
                        val serverChannel = key.channel() as ServerSocketChannel
                        val channel = serverChannel.accept()
                        predecessorActionChannels.add(
                                channel.register(selector, SelectionKey.OP_WRITE, ChannelType.PREDECESSOR_ACTION_WRITE)
                        )
                    }

                    // TODO this is broken!
                    // TODO we don't send the membership list back to the join node!!
                    key.isAcceptable && type == ChannelType.JOIN_ACCEPT ->
                        actions.add(processJoinAndCreateAction(key.channel() as ServerSocketChannel))

                    else -> throw Throwable("unknown channel and operation!")
                }

                selectionKeySet.remove(key)
            }

            if (actions.isNotEmpty()) {
                // update local membership list
                processActions(actions)


                // send actions to collected predecessors
                for (channel in predecessorChannels) {
                    pendingPredecessorActions.remove(channel.remoteAddress)
                            ?.let { list ->
                                pushActionsToPredecessor(channel, list)
                            }
                }

                rebuildRings()
            }
        }
    }

    private fun rebuildRings() {
        TODO()
    }

    private fun processActions(actions: List<Action>) {
        TODO()
    }

    private fun readActions(channel: SocketChannel): List<Action> {
        TODO()
    }

    private fun pushActionsToPredecessor(channel: SocketChannel, actions: List<Action>) {
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

        //TODO()
        val path =  "asdf"
        val inputStream: InputStream = File(path).inputStream()
        val lineList = mutableListOf<String>()

        inputStream.bufferedReader().useLines { lines -> lines.forEach { lineList.add(it)} }

        val newList = mutableListOf<String>()

        val whatismyip = URL("http://checkip.amazonaws.com")
        val buffer = BufferedReader(InputStreamReader(
                whatismyip.openStream()))

        var ip = buffer.readLine() //you get the IP as a String
        ip = ip.replace(".","-")
        ip = "ec2-"+ ip + ".ca-central-1.compute.amazonaws.com"


        val position = lineList.indexOf(ip)
        val size = lineList.size

        newList.add(lineList.elementAt(position%size))
        newList.add(lineList.elementAt((position+1)%size))
        newList.add(lineList.elementAt((position+2)%size))
        return emptyList()
    }

    // connect to successors
    private fun buildSuccessorActionChannels(): List<SocketChannel> {
        if (membershipList.nodes.size < 2) return emptyList()

        // put channels in non-blocking mode
        // channel.configureBlocking(false)
        TODO()
    }

    // send heartbeat
    private fun startSendingHeartbeats() {
        if (membershipList.nodes.size < 2) return

        val nodes = membershipList.nodes
        val successors = getPredecessorIndices().map { nodes[it] }

        heartbeatTimerTask = timer.scheduleAtFixedRate(delay = 0, period = HEARTBEAT_INTERVAL) {
            successors.forEach { successor ->
                DatagramSocket().use { socket ->
                    socket.send(DatagramPacket(byteArrayOf(), 0, successor.addr))
                }
            }
        }
    }

    private fun getPredecessorIndices(): IntArray {
        val nodes = membershipList.nodes

        val size = nodes.size
        val indices = intArrayOf(size - 1, size - 2, size - 3)

        var found = false
        for (i in 0 until size) {
            if (found) break
            if (nodes[i] == self) {
                found = true
            }

            // shuffle everything to the right
            indices[1] = indices[2]
            indices[1] = indices[0]
            indices[0] = i
        }

        return indices
    }

    private fun getSuccessorIndices(): IntArray {
        val nodes = membershipList.nodes

        val size = nodes.size
        val indices = intArrayOf(0, 1, 2)

        var found = false
        for (i in 0 until size) {
            if (found) break
            if (nodes[i] == self) {
                found = true
            }

            // shuffle everything to the right
            indices[1] = indices[2]
            indices[1] = indices[0]
            indices[0] = i
        }

        return indices
    }

    // what to do when you get heartbeat
    private fun processPredecessorHeartbeat(channel: DatagramChannel) {
        println("received packet!")
        channel.receive(ByteBuffer.allocate(10))
//        TODO()
    }

    private fun processJoinAndCreateAction(channel: ServerSocketChannel): Action.Join {
        val node = channel.accept()
                .use { socketChannel ->
                    Node(socketChannel.remoteAddress, Instant.now())
                }

        return Action.Join(node)
    }

    // can return InetAddress/InetSocketAddress
    fun ReturnThreeSuccessors(membershipList: MembershipList): List<Node> {

//        val whatismyip = URL("http://checkip.amazonaws.com")
//        val buffer = BufferedReader(InputStreamReader(whatismyip.openStream()))
//        var ip = buffer.readLine() //you get the IP as a String
//        ip = ip.replace(".","-")
//        ip = "ec2-"+ ip + ".ca-central-1.compute.amazonaws.com"

        val nodes = membershipList.nodes
        val position = nodes.indexOf(getSelfNode())
        val size = nodes.size
        val succssors = mutableListOf<Node>()
        succssors.add(nodes.elementAt(position % size))
        succssors.add(nodes.elementAt((position + 1) % size))
        succssors.add(nodes.elementAt((position + 2) % size))
        return succssors
    }

    fun sendMessage(channel: SocketChannel, type:Actions.Reuqest.Type, node:Node){
        // set buffer
        val byteBuffer = ByteBuffer.allocate(1024)
        // build proto
        val action = Actions.Reuqest.newBuilder()
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

        val parsed = Actions.Reuqest.parseFrom(buf)
        when (parsed.getType()){
            Actions.Reuqest.Type.JOIN ->
                println("joined")
            Actions.Reuqest.Type.REMOVE ->
                println("removed")
            Actions.Reuqest.Type.DROP ->
                println("dropped")
            else ->
                println("else")
        }
    }

}