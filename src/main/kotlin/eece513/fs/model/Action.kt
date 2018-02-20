package eece513.fs.model

sealed class Action {
    abstract val node: Node

    data class Join(override val node: Node) : Action()

    data class Leave(override val node: Node) : Action()

    data class Drop(override val node: Node) : Action()

    data class Connect(override val node: Node): Action()

    data class Heartbeat(override val node: Node): Action()
}