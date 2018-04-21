package eece513.fs.util

import eece513.common.model.Action
import eece513.common.model.Node
import eece513.fs.DummyLogger
import org.junit.Test

import org.junit.Assert.*
import java.net.InetSocketAddress
import java.time.Instant
import java.util.*

class SuccessorSentActionsTest {

    @Test
    fun add() {
        val node = Node(InetSocketAddress("127.0.0.1", 6969), Instant.now())
        val list = LinkedList<Action>()
        val ssa = SuccessorSentActions(list, 2, DummyLogger())

        // add three actions
        ssa.add(Action.ClusterAction.Join(node))
        ssa.add(Action.ClusterAction.Join(node))
        ssa.add(Action.ClusterAction.Join(node))

        assertTrue(list.size == 2)
    }

    @Test
    fun contains() {
        val node = Node(InetSocketAddress("127.0.0.1", 6969), Instant.now())
        val ssa = SuccessorSentActions(DummyLogger(), 2)

        assertFalse(ssa.contains(Action.ClusterAction.Join(node)))
        ssa.add(Action.ClusterAction.Join(node))
        assertTrue(ssa.contains(Action.ClusterAction.Join(node)))
    }
}