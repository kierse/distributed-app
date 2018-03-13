package eece513.common.model

sealed class ConnectionPurpose {
    class NodeJoin : ConnectionPurpose()
    class ClientGet : ConnectionPurpose()
    class ClientPut: ConnectionPurpose()
    class ClientRemove: ConnectionPurpose()
}