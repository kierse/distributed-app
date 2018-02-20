package eece513.fs.util

import eece513.fs.SUCCESSOR_SENT_ACTIONS_LIMIT
import eece513.fs.model.Action
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