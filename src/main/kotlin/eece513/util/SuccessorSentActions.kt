package eece513.util

import eece513.SUCCESSOR_SENT_ACTIONS_LIMIT
import eece513.model.Action
import java.util.*

class SuccessorSentActions internal constructor(
        private val actions: LinkedList<Action>, private val size: Int
) {
    constructor(size: Int = SUCCESSOR_SENT_ACTIONS_LIMIT): this(LinkedList(), size)

    fun add(action: Action) {
        if (actions.size >= size) {
            actions.removeFirst()
        }

        actions.add(action)
    }

    fun contains(action: Action): Boolean = actions.contains(action)
}