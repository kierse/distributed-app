package eece513.model

sealed class Action {
    enum class Type {
        JOIN, LEAVE, DROP, CONNECT, HEARTBEAT
    }

    abstract val type: Type
    abstract val node: Node

    data class Join(override val node: Node) : Action() {
        override val type = Type.JOIN
    }

    data class Leave(override val node: Node) : Action() {
        override val type = Type.LEAVE
    }

    data class Drop(override val node: Node) : Action() {
        override val type = Type.DROP
    }

    data class Connect(override val node: Node): Action() {
        override val type = Type.CONNECT
    }

    data class Heartbeat(override val node: Node): Action() {
        override val type = Type.HEARTBEAT
    }
}