package eece513.fs.ring

import com.nhaarman.mockito_kotlin.*
import eece513.fs.DummyLogger
import eece513.fs.Logger
import eece513.fs.PredecessorHeartbeatMonitorController
import eece513.fs.channel.*
import eece513.fs.model.Action
import eece513.fs.model.MembershipList
import eece513.fs.model.Node
import eece513.fs.util.SuccessorSentActions
import kotlinx.coroutines.experimental.channels.Channel
import org.junit.Assert.*
import org.junit.Test

import java.net.InetSocketAddress
import java.time.Instant

class RingImplTest {
    private val node = Node(InetSocketAddress("127.0.0.1", 6969), Instant.now())
    private val logger = DummyLogger()

    @Test
    fun processActionsFromPredecessor__empty_list() {
        val membershipList = MembershipList(listOf(node))

        val reader = mock<ReadActionChannel>()
        whenever(reader.read()).thenReturn(null)

        val ring = RingImpl(node, membershipList, Channel(), logger)
        ring.processActionsFromPredecessor(node, reader)

        assertEquals(membershipList, ring.membershipList)
    }

    @Test
    fun processActionsFromPredecessor__filter_leave_to_drop() {
        val newNode = Node(InetSocketAddress("127.0.0.1", 6970), Instant.now())
        val list = MembershipList(listOf(node, newNode))

        val reader = mock<ReadActionChannel>()
        whenever(reader.read()).thenReturn(Action.Leave(newNode), null)

        val ring = RingImpl(node, list, Channel(), logger)
        ring.processActionsFromPredecessor(node, reader)

        assertEquals(MembershipList(listOf(node)), ring.membershipList)
    }

    @Test
    fun processActionsFromPredecessor__filter_self_join() {
        val reader = mock<ReadActionChannel>()
        whenever(reader.read()).thenReturn(Action.Join(node), null)

        val ring = RingImpl(node, Channel(), logger)

        ring.addSuccessor(node)
        ring.processActionsFromPredecessor(node, reader)

        assertTrue(ring.pendingSuccessorActions.getValue(node).isEmpty())
    }

    @Test
    fun processActionsFromPredecessor__processAction_duplicate_join() {
        val list = MembershipList(listOf(node))

        val reader = mock<ReadActionChannel>()
        whenever(reader.read()).thenReturn(Action.Join(node), null)

        val ring = RingImpl(node, list, Channel(), logger)

        ring.addSuccessor(node)
        ring.processActionsFromPredecessor(node, reader)

        assertEquals(list, ring.membershipList)
    }

    @Test
    fun processActionsFromPredecessor__processAction_join() {
        val newNode = Node(InetSocketAddress("127.0.0.1", 6970), Instant.now())
        val list = MembershipList(listOf(node))

        val reader = mock<ReadActionChannel>()
        whenever(reader.read()).thenReturn(Action.Join(newNode), null)

        val ring = RingImpl(node, list, Channel(), logger)

        ring.addSuccessor(node)
        ring.processActionsFromPredecessor(node, reader)

        assertNotEquals(list, ring.membershipList)
        assertTrue(ring.membershipList.nodes.contains(newNode))
    }

    @Test
    fun processActionsFromPredecessor__processAction_join_rebuildRing() {
        val newNode = Node(InetSocketAddress("127.0.0.1", 6970), Instant.now())
        val list = MembershipList(listOf(node))

        val reader = mock<ReadActionChannel>()
        whenever(reader.read()).thenReturn(Action.Join(newNode), null)

        val ring = RingImpl(node, list, Channel(), logger)

        ring.addSuccessor(node)
        ring.processActionsFromPredecessor(node, reader)

        assertTrue(ring.rebuildRing)
    }

    @Test
    fun processActionsFromPredecessor__processAction_drop() {
        val newNode = Node(InetSocketAddress("127.0.0.1", 6970), Instant.now())
        val list = MembershipList(listOf(node, newNode))

        val reader = mock<ReadActionChannel>()
        whenever(reader.read()).thenReturn(Action.Drop(newNode), null)

        val ring = RingImpl(node, list, Channel(), logger)
        ring.processActionsFromPredecessor(node, reader)

        assertEquals(MembershipList(listOf(node)), ring.membershipList)
    }

    @Test
    fun processActionsFromPredecessor__processAction_drop_rebuildRing() {
        val newNode = Node(InetSocketAddress("127.0.0.1", 6970), Instant.now())
        val list = MembershipList(listOf(node, newNode))

        val reader = mock<ReadActionChannel>()
        whenever(reader.read()).thenReturn(Action.Drop(newNode), null)

        val ring = RingImpl(node, list, Channel(), logger)
        ring.processActionsFromPredecessor(node, reader)

        assertTrue(ring.rebuildRing)
    }

    @Test(expected = IllegalStateException::class)
    fun processActionsFromPredecessor__processAction_heartbeat() {
        val list = MembershipList(listOf(node))

        val reader = mock<ReadActionChannel>()
        whenever(reader.read()).thenReturn(Action.Heartbeat(node), null)

        val ring = RingImpl(node, list, Channel(), logger)
        ring.processActionsFromPredecessor(node, reader)
    }

    @Test(expected = IllegalStateException::class)
    fun processActionsFromPredecessor__processAction_connect() {
        val list = MembershipList(listOf(node))

        val reader = mock<ReadActionChannel>()
        whenever(reader.read()).thenReturn(Action.Connect(node), null)

        val ring = RingImpl(node, list, Channel(), logger)
        ring.processActionsFromPredecessor(node, reader)
    }

    @Test
    fun processActionsFromPredecessor__add_to_pending_actions() {
        val action = Action.Drop(node)

        val reader = mock<ReadActionChannel>()
        whenever(reader.read()).thenReturn(action, null)

        val ring = RingImpl(node, Channel(), logger)

        ring.addSuccessor(node)
        ring.processActionsFromPredecessor(node, reader)

        assertTrue(ring.pendingSuccessorActions.getValue(node).contains(action))
    }

    @Test
    fun sendActionsToSuccessor__queue_actions() {
        val actions = mutableListOf(
                Action.Join(node),
                Action.Drop(node)
        )

        val sender = mock<BufferedSendActionChannel>()
        whenever(sender.send()).doReturn(actions.map { true }.plus(false))

        val ring = RingImpl(node, mock(), logger)
        ring.pendingSuccessorActions[node] = actions
        ring.sentSuccessorActions[node] = SuccessorSentActions()

        ring.sendActionsToSuccessor(node, sender)

        verify(sender).queue(actions[0])
        verify(sender).queue(actions[1])
    }

    @Test
    fun sendActionsToSuccessor__send_actions_to_channel() {
        val actions: MutableList<Action> = mutableListOf(Action.Join(node))

        val sender = mock<BufferedSendActionChannel>()
        whenever(sender.send()).thenReturn(true, false)

        val ring = RingImpl(node, mock(), logger)
        ring.pendingSuccessorActions[node] = actions
        ring.sentSuccessorActions[node] = SuccessorSentActions()

        ring.sendActionsToSuccessor(node, sender)

        verify(sender, atLeastOnce()).send()
    }

    @Test
    fun sendActionsToSuccessor__send_actions_until_channel_returns_false() {
        val sender = mock<BufferedSendActionChannel>()
        whenever(sender.send()).thenReturn(true, true, true, false)

        val ring = RingImpl(node, mock(), logger)

        ring.addSuccessor(node)
        ring.sendActionsToSuccessor(node, sender)

        verify(sender, times(4)).send()
    }

    @Test
    fun sendActionsToSuccessor__move_pending_actions_to_sent_actions() {
        val actions = mutableListOf(
                Action.Join(node),
                Action.Drop(node)
        )

        val sender = mock<BufferedSendActionChannel>()
        whenever(sender.send()).thenReturn(true, false)

        val ring = RingImpl(node, Channel(), logger)
        ring.pendingSuccessorActions[node] = actions
        ring.sentSuccessorActions[node] = SuccessorSentActions()

        ring.sendActionsToSuccessor(node, sender)

        assertTrue(ring.pendingSuccessorActions.getValue(node).isEmpty())
        assertTrue(ring.sentSuccessorActions.getValue(node).contains(actions[0]))
        assertTrue(ring.sentSuccessorActions.getValue(node).contains(actions[1]))
    }

    @Test
    fun sendActionsToSuccessor__no_actions_to_send() {
        val sender = mock<BufferedSendActionChannel>()
        whenever(sender.send()).thenReturn(false)

        val ring = RingImpl(node, Channel(), logger)

        ring.addSuccessor(node)
        ring.sendActionsToSuccessor(node, sender)

        verify(sender, never()).queue(any())
    }


    @Test
    fun addSuccessor() {
        val ring = RingImpl(node, mock(), logger)

        ring.addSuccessor(node)

        assertTrue(ring.pendingSuccessorActions.containsKey(node))
        assertTrue(ring.sentSuccessorActions.containsKey(node))
    }

    @Test
    fun removeSuccessor() {
        val ring = RingImpl(node, mock(), logger)

        ring.addSuccessor(node)
        ring.removeSuccessor(node)

        assertFalse(ring.pendingSuccessorActions.containsKey(node))
        assertFalse(ring.sentSuccessorActions.containsKey(node))
    }

    @Test
    fun dropPredecessor() {
        val ring = RingImpl(node, mock(), logger)

        ring.addSuccessor(node)
        ring.dropPredecessor(node)

        assertTrue(ring.pendingSuccessorActions.getValue(node).contains(Action.Drop(node)))
    }

    @Test
    fun processJoinRequest__empty_read_action_channel() {
        assertFalse(RingImpl(node, mock(), logger).processJoinRequest(mock()))
    }

    @Test
    fun processJoinRequest__add_to_pending_actions() {
        val a = Node(InetSocketAddress("127.0.0.1", 6970), Instant.now())
        val newNode = Node(InetSocketAddress("127.0.0.1", 6971), Instant.now())
        val join = Action.Join(newNode)

        val channel = mock<ReadActionChannel>()
        whenever(channel.read()).thenReturn(join, null)

        val ring = RingImpl(node, mock(), logger)
        ring.addSuccessor(node)
        ring.addSuccessor(a)
        ring.processJoinRequest(channel)

        assertTrue(ring.pendingSuccessorActions.getValue(node).contains(join))
        assertTrue(ring.pendingSuccessorActions.getValue(a).contains(join))
    }

    @Test
    fun processJoinRequest() {
        val newNode = Node(InetSocketAddress("127.0.0.1", 6970), Instant.now())

        val channel = mock<ReadActionChannel>()
        whenever(channel.read()).thenReturn(Action.Join(newNode), null)

        val ring = RingImpl(node, mock(), logger)

        assertTrue(ring.processJoinRequest(channel))
        assertEquals(MembershipList(listOf(node, newNode)), ring.membershipList)
    }

    @Test
    fun sendMembershipList() {
        val sender = mock<SendMembershipListChannel>()
        val membershipList = MembershipList(listOf(node))
        RingImpl(node, membershipList, Channel(), logger).sendMembershipList(sender)

        verify(sender).send(membershipList)
    }

    @Test
    fun sendJoinRequest() {
        val sender = mock<SendActionChannel>()
        RingImpl(node, Channel(), logger).sendJoinRequest(sender)

        verify(sender).send(Action.Join(node))
    }

    @Test
    fun readMembershipList__empty_channel() {
        assertFalse(RingImpl(node, mock(), logger).readMembershipList(mock()))
    }

    @Test
    fun readMembershipList() {
        val newList = MembershipList(listOf(
                node, Node(InetSocketAddress("127.0.0.1", 6970), Instant.now())
        ))

        val reader = mock<ReadMembershipListChannel>()
        whenever(reader.read()).thenReturn(newList)

        val ring = RingImpl(node, mock(), logger)

        ring.readMembershipList(reader)

        assertEquals(newList, ring.membershipList)
    }

    @Test
    fun sendIdentity() {
        val sender = mock<SendActionChannel>()
        RingImpl(node, Channel(), logger).sendIdentity(sender)

        verify(sender).send(Action.Connect(node))
    }

    @Test
    fun readIdentity() {
        val reader = mock<ReadActionChannel>()
        whenever(reader.read()).thenReturn(Action.Connect(node))

        assertEquals(node, RingImpl(node, Channel(), logger).readIdentity(reader))
    }

    @Test
    fun readIdentity__empty_channel() {
        assertNull(RingImpl(node, Channel(), logger).readIdentity(mock()))
    }

    @Test
    fun processHeartbeat__empty_channel() {
        val heartbeatChannel = mock<Channel<PredecessorHeartbeatMonitorController.Heartbeat>>()

        RingImpl(node, heartbeatChannel, logger).processHeartbeat(mock())

        verify(heartbeatChannel, never()).offer(any())
    }

    @Test
    fun processHeartbeat() {
        val reader = mock<ReadHeartbeatChannel>()
        whenever(reader.read()).thenReturn(node)

        val heartbeatChannel = Channel<PredecessorHeartbeatMonitorController.Heartbeat>(1)

        RingImpl(node, heartbeatChannel, logger).processHeartbeat(reader)

        assertEquals(PredecessorHeartbeatMonitorController.Heartbeat(node), heartbeatChannel.poll())
    }

    @Test
    fun processHeartbeat__coroutine_channel_full() {
        val reader = mock<ReadHeartbeatChannel>()
        whenever(reader.read()).thenReturn(node)

        val logger = mock<Logger>()

        RingImpl(node, Channel(), logger).processHeartbeat(reader)

        verify(logger).warn(any(), any())
    }

    @Test
    fun processMissedHeartbeats() {
        val a = Node(InetSocketAddress("127.0.0.1", 6970), Instant.now())
        val b = Node(InetSocketAddress("127.0.0.1", 6971), Instant.now())
        val c = Node(InetSocketAddress("127.0.0.1", 6972), Instant.now())

        val list = MembershipList(listOf(node, a, b, c))

        val reader = mock<MissedHeartbeatChannel>()
        whenever(reader.read()).thenReturn(a, b, c, null)

        val ring = RingImpl(node, list, Channel(), logger)
        ring.processMissedHeartbeats(reader)

        assertEquals(MembershipList(listOf(node)), ring.membershipList)
    }

    @Test
    fun leave() {
        val a = Node(InetSocketAddress("127.0.0.1", 6970), Instant.now())
        val ring = RingImpl(node, Channel(), logger)

        ring.addSuccessor(a)
        ring.leave()

        assertTrue(ring.pendingSuccessorActions.getValue(a).contains(Action.Leave(node)))
    }

    @Test
    fun getPredecessors__three() {
        val a = Node(InetSocketAddress("127.0.0.1", 6970), Instant.now())
        val b = Node(InetSocketAddress("127.0.0.1", 6971), Instant.now())
        val c = Node(InetSocketAddress("127.0.0.1", 6972), Instant.now())

        val list = MembershipList(listOf(node, a, b, c))

        val ring = RingImpl(node, list, Channel(), logger)

        assertEquals(listOf(c, b, a), ring.getPredecessors())
    }

    @Test
    fun getPredecessors__two() {
        val a = Node(InetSocketAddress("127.0.0.1", 6970), Instant.now())
        val b = Node(InetSocketAddress("127.0.0.1", 6971), Instant.now())

        val list = MembershipList(listOf(node, a, b))

        val ring = RingImpl(node, list, Channel(), logger)

        assertEquals(listOf(b, a), ring.getPredecessors())
    }

    @Test
    fun getPredecessors__one() {
        val a = Node(InetSocketAddress("127.0.0.1", 6970), Instant.now())

        val list = MembershipList(listOf(node, a))

        val ring = RingImpl(node, list, Channel(), logger)

        assertEquals(listOf(a), ring.getPredecessors())
    }

    @Test
    fun getPredecessors__none() {
        val list = MembershipList(listOf(node))

        val ring = RingImpl(node, list, Channel(), logger)

        assertEquals(emptyList<Node>(), ring.getPredecessors())
    }

    @Test
    fun getSuccessors__three() {
        val a = Node(InetSocketAddress("127.0.0.1", 6970), Instant.now())
        val b = Node(InetSocketAddress("127.0.0.1", 6971), Instant.now())
        val c = Node(InetSocketAddress("127.0.0.1", 6972), Instant.now())

        val list = MembershipList(listOf(node, a, b, c))

        val ring = RingImpl(node, list, Channel(), logger)

        assertEquals(listOf(a, b, c), ring.getSuccessors())
    }

    @Test
    fun getSuccessors__two() {
        val a = Node(InetSocketAddress("127.0.0.1", 6970), Instant.now())
        val b = Node(InetSocketAddress("127.0.0.1", 6971), Instant.now())

        val list = MembershipList(listOf(node, a, b))

        val ring = RingImpl(node, list, Channel(), logger)

        assertEquals(listOf(a, b), ring.getSuccessors())
    }

    @Test
    fun getSuccessors__one() {
        val a = Node(InetSocketAddress("127.0.0.1", 6970), Instant.now())

        val list = MembershipList(listOf(node, a))

        val ring = RingImpl(node, list, Channel(), logger)

        assertEquals(listOf(a), ring.getSuccessors())
    }

    @Test
    fun getSuccessors__empty() {
        val list = MembershipList(listOf(node))

        val ring = RingImpl(node, list, Channel(), logger)

        assertEquals(emptyList<Node>(), ring.getSuccessors())
    }
}