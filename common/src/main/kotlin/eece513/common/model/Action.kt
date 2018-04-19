package eece513.common.model

sealed class Action {
    sealed class ClusterAction : Action() {
        abstract val node: Node

        data class Join(override val node: Node) : ClusterAction()

        data class Leave(override val node: Node) : ClusterAction()

        data class Drop(override val node: Node) : ClusterAction()

        data class Connect(override val node: Node): ClusterAction()

        data class Heartbeat(override val node: Node): ClusterAction()
    }

    sealed class FileSystemAction : Action() {
        abstract val sender: Node
        data class RemoveFile(val remoteName: String, override val sender: Node) : FileSystemAction()
    }
}

