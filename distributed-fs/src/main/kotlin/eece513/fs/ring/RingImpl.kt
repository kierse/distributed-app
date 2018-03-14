package eece513.fs.ring

import eece513.fs.Logger
import eece513.fs.PredecessorHeartbeatMonitorController
import eece513.fs.channel.*
import eece513.fs.model.Action
import eece513.fs.model.MembershipList
import eece513.fs.model.Node
import eece513.fs.util.SuccessorSentActions
import kotlinx.coroutines.experimental.channels.SendChannel

interface Ring {
    val rebuildRing: Boolean

    fun addSuccessor(successor: Node)
    fun removeSuccessor(successor: Node)

    fun sendJoinRequest(channel: SendActionChannel): Boolean
    fun readMembershipList(channel: ReadMembershipListChannel): Boolean

    fun leave()

    fun processJoinRequest(channel: ReadActionChannel): Boolean
    fun sendMembershipList(channel: SendMembershipListChannel): Boolean

    fun sendIdentity(channel: SendActionChannel): Boolean
    fun readIdentity(channel: ReadActionChannel): Node?

    fun sendActionsToSuccessor(successor: Node, channel: BufferedSendActionChannel)
    fun processActionsFromPredecessor(predecessor: Node, channel: ReadActionChannel)

    fun processHeartbeat(channel: ReadHeartbeatChannel)
    fun processMissedHeartbeats(channel: MissedHeartbeatChannel)

    fun dropPredecessor(predecessor: Node)

    fun getPredecessors(): List<Node>
    fun getSuccessors(): List<Node>
}

interface MutableRing {
    val pendingSuccessorActions: MutableMap<Node, MutableList<Action>>
    val sentSuccessorActions: MutableMap<Node, SuccessorSentActions>
    var membershipList: MembershipList
}

class RingImpl /* testing */ constructor(
        private val self: Node,
        override var membershipList: MembershipList,
        private val missedHeartbeatChannel: SendChannel<PredecessorHeartbeatMonitorController.Heartbeat>,
        private val logger: Logger
) : Ring, MutableRing {
    private val tag = RingImpl::class.java.simpleName

    override val pendingSuccessorActions = mutableMapOf<Node, MutableList<Action>>()
    override val sentSuccessorActions = mutableMapOf<Node, SuccessorSentActions>()
    override var rebuildRing: Boolean = false
        get() = field.also { field = false }

    constructor(
            self: Node,
            missedHeartbeatChannel: SendChannel<PredecessorHeartbeatMonitorController.Heartbeat>,
            logger: Logger
    ) : this(self, MembershipList(listOf(self)), missedHeartbeatChannel, logger)

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
            action is Action.Leave -> {
                logger.info(tag, "received leave from ${action.node}")
                Action.Drop(action.node)
            }

            action is Action.Join && action.node == self -> {
                logger.debug(tag, "ignoring self join action")
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
        while (channel.send()) { /* do nothing */
        }
    }

    private fun processAction(action: Action) {
        val nodes = when (action) {
            is Action.Join -> {
                if (membershipList.nodes.contains(action.node)) {
                    logger.debug(tag, "ignoring duplicate join action for ${action.node}")
                    return
                }

                logger.debug(tag, "adding ${action.node} to membership list")
                membershipList.nodes.plus(action.node)
            }

            is Action.Drop -> {
                logger.debug(tag, "dropping ${action.node.addr} from membership list")
                membershipList.nodes.filter { it != action.node }
            }

            is Action.Leave -> throw IllegalStateException("LEAVE actions should not be handled here!")
            is Action.Heartbeat -> throw IllegalStateException("HEARTBEAT actions should not be handled here!")
            is Action.Connect -> throw IllegalStateException("CONNECT actions should not be handled here!")
        }

        if (membershipList.nodes == nodes) return

        membershipList = MembershipList(nodes)
        logger.info(tag, "new membership list: $membershipList")

        rebuildRing = true
    }

    override fun addSuccessor(successor: Node) {
        pendingSuccessorActions[successor] = mutableListOf()
        sentSuccessorActions[successor] = SuccessorSentActions()
    }

    override fun removeSuccessor(successor: Node) {
        pendingSuccessorActions.remove(successor)
        sentSuccessorActions.remove(successor)
    }

    override fun dropPredecessor(predecessor: Node) {
        val dropAction = Action.Drop(predecessor)

        processAction(dropAction)
        pendingSuccessorActions.values.forEach { it.add(dropAction) }
    }

    override fun processJoinRequest(channel: ReadActionChannel): Boolean {
        val joinAction = channel.read() ?: return false

        logger.info(tag, "received join request from ${joinAction.node}")
        processAction(joinAction)
        pendingSuccessorActions.values.forEach { it.add(joinAction) }
        return true
    }

    // Type.JOIN_ACCEPT_WRITE
    override fun sendMembershipList(channel: SendMembershipListChannel): Boolean = channel.send(membershipList)

    override fun sendJoinRequest(channel: SendActionChannel) = channel.send(Action.Join(self))

    override fun readMembershipList(channel: ReadMembershipListChannel): Boolean {
        val newList = channel.read() ?: return false

        logger.info(tag, "received membership list: $newList")
        membershipList = newList
        rebuildRing = true
        return true
    }

    override fun sendIdentity(channel: SendActionChannel): Boolean = channel.send(Action.Connect(self))

    override fun readIdentity(channel: ReadActionChannel): Node? = channel.read()?.node

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
        val leaveAction = Action.Leave(self)
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
}