package eece513.fs.util

import eece513.common.Logger
import eece513.fs.SUCCESSOR_SENT_ACTIONS_LIMIT
import eece513.common.model.Action
import java.util.*

class SuccessorSentActions internal constructor(
        private val actions: LinkedList<Action>, private val size: Int, private val logger: Logger
) {
    constructor(size: Int = SUCCESSOR_SENT_ACTIONS_LIMIT, logger: Logger) : this(LinkedList(), size, logger)

    fun add(action: Action) {
        if (actions.size >= size) {
            actions.removeFirst()
        }

        logger.debug("SuccessorSentActions", "adding $action to list")
        actions.add(action)
    }

    fun contains(action: Action): Boolean = actions.contains(action)
}