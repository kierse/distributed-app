package eece513.fs.ring

import eece513.common.Logger
import eece513.fs.PredecessorHeartbeatMonitorController
import eece513.fs.channel.*
import eece513.common.model.Action
import eece513.common.model.FsCommand
import eece513.common.model.FsResponse
import eece513.fs.model.MembershipList
import eece513.common.model.Node
import eece513.fs.util.SuccessorSentActions
import kotlinx.coroutines.experimental.channels.SendChannel
import java.time.Instant
import java.util.*

class RingImpl /* testing */ constructor(
        private val self: Node,
        private val fileSystem: FileSystem,
        list: MembershipList,
        private val missedHeartbeatChannel: SendChannel<PredecessorHeartbeatMonitorController.Heartbeat>,
        private val logger: Logger
) : Ring {
    private val tag = RingImpl::class.java.simpleName

    var membershipList: MembershipList = list
        set(newList) {
            fileSystem.saveAddressListToDisk(newList.nodes.map { it.addr.hostName })
            field = newList
        }

    val pendingSuccessorActions = mutableMapOf<Node, MutableList<Action>>()
    val sentSuccessorActions = mutableMapOf<Node, SuccessorSentActions>()

    override var rebuildRing: Boolean = false
        get() = field.also { field = false }

    constructor(
            self: Node,
            fileSystem: FileSystem,
            missedHeartbeatChannel: SendChannel<PredecessorHeartbeatMonitorController.Heartbeat>,
            logger: Logger
    ): this(self, fileSystem, MembershipList(listOf(self)), missedHeartbeatChannel, logger)

    fun initialize() {
        fileSystem.initialize()
    }

    // SelectionKey.OP_READ && Type.PREDECESSOR_ACTION_READ
    override fun processActionsFromPredecessor(predecessor: Node, channel: ReadActionChannel) {
        val usableActions = mutableListOf<Action>()
        do {
            val receivedAction = channel.read()
                    ?.let { filterAction(it) }
                    ?.also {
                        processAction(it)
                        usableActions.add(it)
                    }
        } while (receivedAction != null)

        if (usableActions.isNotEmpty()) {
            pendingSuccessorActions.values.forEach { it.addAll(usableActions) }
        }
    }

    private fun filterAction(action: Action): Action? {
        return when {
            action is Action.ClusterAction.Leave -> {
                logger.info(tag, "received leave from ${action.node}")
                Action.ClusterAction.Drop(action.node)
            }

            action is Action.ClusterAction.Join && action.node == self -> {
                logger.debug(tag, "ignoring self join action")
                null
            }

            action is Action.FileSystemAction.RemoveFile && action.sender == self -> {
                logger.debug(tag, "ignoring remove file action")
                null
            }

            else -> action
        }
    }

    // SelectionKey.OP_WRITE && Type.SUCCESSOR_ACTION_WRITE
    override fun sendActionsToSuccessor(successor: Node, channel: BufferedSendActionChannel) {
        val sentActions = sentSuccessorActions.getValue(successor)

        val pending = pendingSuccessorActions.remove(successor) ?: emptyList<Action>()
        pendingSuccessorActions[successor] = mutableListOf()

        for (action in pending) {
            // If the current action exists in sentSuccessorActions, remove it. It has
            // completed a full circle around the ring and doesn't need to be pushed to
            // this successor
            if (sentActions.contains(action)) {
                logger.debug(tag, "removing $action as we've already sent it to $successor")
                continue
            }

            channel.queue(action)
            sentActions.add(action)
        }

        // repeated send queued actions until all are sent or channel is full
        while (channel.send()) { /* do nothing */ }
    }

    private fun processAction(action: Action) {
        when (action) {
            is Action.ClusterAction -> processClusterAction(action)
            is Action.FileSystemAction.RemoveFile -> processFileSystemAction(action)
            else -> throw IllegalArgumentException("Unknown action!")
        }
    }

    private fun processClusterAction(action: Action.ClusterAction) {
        val nodes = when (action) {
            is Action.ClusterAction.Join -> {
                if (membershipList.nodes.contains(action.node)) {
                    logger.debug(tag, "ignoring duplicate join action for ${action.node}")
                    return
                }

                logger.debug(tag, "adding ${action.node} to membership list")
                membershipList.nodes.plus(action.node)
            }

            is Action.ClusterAction.Drop -> {
                logger.debug(tag, "dropping ${action.node.addr} from membership list")
                membershipList.nodes.filter { it != action.node }
            }

            is Action.ClusterAction.Leave -> throw IllegalStateException("LEAVE actions should not be handled here!")
            is Action.ClusterAction.Heartbeat -> throw IllegalStateException("HEARTBEAT actions should not be handled here!")
            is Action.ClusterAction.Connect -> throw IllegalStateException("CONNECT actions should not be handled here!")
        }

        if (membershipList.nodes == nodes) return

        membershipList = MembershipList(nodes)
        logger.info(tag, "new membership list: $membershipList")

        rebuildRing = true
    }

    private fun processFileSystemAction(action: Action.FileSystemAction.RemoveFile) {
        fileSystem.removeFile(action.remoteName)
    }

    override fun addSuccessor(successor: Node) {
        pendingSuccessorActions[successor] = mutableListOf()
        sentSuccessorActions[successor] = SuccessorSentActions(logger = logger)
    }

    override fun removeSuccessor(successor: Node) {
        pendingSuccessorActions.remove(successor)
        sentSuccessorActions.remove(successor)
    }

    override fun dropPredecessor(predecessor: Node) {
        val dropAction = Action.ClusterAction.Drop(predecessor)

        processClusterAction(dropAction)
        pendingSuccessorActions.values.forEach { it.add(dropAction) }
    }

    override fun processJoinRequest(channel: ReadClusterActionChannel): Boolean {
        val joinAction: Action.ClusterAction.Join = channel.readTyped() ?: return false

        logger.info(tag, "received join request from ${joinAction.node}")
        processClusterAction(joinAction)
        pendingSuccessorActions.values.forEach { it.add(joinAction) }
        return true
    }

    // Type.JOIN_ACCEPT_WRITE
    override fun sendMembershipList(channel: SendMembershipListChannel): Boolean = channel.send(membershipList)

    override fun sendJoinRequest(channel: SendClusterActionChannel) = channel.send(Action.ClusterAction.Join(self))

    override fun readMembershipList(channel: ReadMembershipListChannel): Boolean {
        val newList: MembershipList = channel.readTyped() ?: return false

        logger.info(tag, "received membership list: $newList")
        membershipList = newList
        rebuildRing = true

        fileSystem.syncFiles(membershipList.nodes.first().addr)

        return true
    }

    override fun sendIdentity(channel: SendClusterActionChannel): Boolean = channel.send(Action.ClusterAction.Connect(self))

    override fun readIdentity(channel: ReadClusterActionChannel): Node? = channel.readTyped<Action.ClusterAction.Connect>()?.node

    override fun processHeartbeat(channel: ReadHeartbeatChannel) {
        val predecessor = channel.read() ?: return

        // TODO a malicious node flooding the network with heartbeats will fill the channel buffer
        // TODO and lead to dropped predecessors! Consider fixing this!
        if (missedHeartbeatChannel.offer(PredecessorHeartbeatMonitorController.Heartbeat(predecessor))) return

        // TODO if we get here we're dropping the heartbeat. Is that right??
        // TODO should we block instead?
        logger.warn(tag, "unable to send heartbeat for $predecessor - channel is full!")
    }

    override fun processMissedHeartbeats(channel: MissedHeartbeatChannel) {
        do {
            val predecessor = channel.read()
                    ?.also {
                        dropPredecessor(it)
                    }
        } while (predecessor != null)
    }

    override fun leave() {
        val leaveAction = Action.ClusterAction.Leave(self)
        pendingSuccessorActions.values.forEach { it.add(leaveAction) }
    }

    override fun getPredecessors(): List<Node> {
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

    override fun getSuccessors(): List<Node> {
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

    override fun processFileRemove(channel: ReadFsCommandChannel): Boolean {
        val removeCommand: FsCommand.FsRemove = channel.readTyped() ?: return false
        val action = Action.FileSystemAction.RemoveFile(removeCommand.remoteFile, self)

        logger.info(tag, "Received $removeCommand")
        processFileSystemAction(action)
        pendingSuccessorActions.values.forEach { it.add(action) }
        return true
    }

    override fun sendFileRemoveResponse(channel: SendFsResponseChannel): Boolean {
        return channel.send(FsResponse.RemoveResponse())
    }

    override fun processFileGet(channel: ReadFsCommandChannel): FsResponse? {
        val getCommand: FsCommand.FsGet = channel.readTyped() ?: return null
        logger.info(tag, "Received $getCommand")

        val file = fileSystem.getLatestVersion(getCommand.remoteFile)
        if (file == null) {
            logger.warn(tag, "Unable to find ${getCommand.remoteFile}")
            return FsResponse.UnknownFile()
        }

        logger.info(tag, "Latest version: ${file.name}")
        return FsResponse.GetResponse(file.absolutePath)
    }

    override fun sendFileGetResponse(channel: FsResponseStateChannel): Boolean {
        return channel.sendChannel.send(channel.response)
    }

    override fun processFilePut(channel: ReadFsCommandChannel): FsResponse? {
        val putCommand: FsCommand.FsPut = channel.readTyped() ?: return null
        logger.info(tag, "Received $putCommand")

        val now = Instant.now()
        val file = fileSystem.buildEncodedFileName(putCommand.remoteFile, now)

        val response: FsResponse = if (fileSystem.clientShouldPromptUser(putCommand.remoteFile, now)) {
            FsResponse.PutAckWithPrompt(file.absolutePath)
        } else {
            FsResponse.PutAck(file.absolutePath)
        }

        fileSystem.registerPutRequest(putCommand.remoteFile, now)

        return response
    }

    override fun processFilePutConfirm(channel: ReadFsCommandChannel): Boolean {
        val confirmCommand: FsCommand.PutConfirm = channel.readTyped() ?: return false

        fileSystem.confirmPut(confirmCommand.pathToFile)

        return true
    }

    override fun sendFilePutResponse(channel: FsResponseStateChannel): Boolean {
        return channel.sendChannel.send(channel.response)
    }
}
