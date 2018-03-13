package eece513.fs.channel

interface RingChannel {
    val type: Type

    enum class Type {
        NODE_ACCEPT,
        NODE_ACCEPT_READ,
        JOIN_ACCEPT_READ,
        JOIN_ACCEPT_WRITE,
        JOIN_CONNECT,
        JOIN_CONNECT_ACTION_WRITE,
        JOIN_CONNECT_PURPOSE_WRITE,
        JOIN_CONNECT_READ,
        CLIENT_GET,
        CLIENT_GET_WRITE,
        CLIENT_REMOVE,
        CLIENT_REMOVE_WRITE,
        CLIENT_PUT,
        CLIENT_PUT_CONFIRM,
        CLIENT_PUT_WRITE,
        PREDECESSOR_ACCEPT,
        PREDECESSOR_ACTION_READ,
        PREDECESSOR_ACTION_WRITE,
        PREDECESSOR_CONNECT_WRITE,
        PREDECESSOR_CONNECT,
        PREDECESSOR_HEARTBEAT_READ,
        PREDECESSOR_MISSED_HEARTBEAT_READ,
        SUCCESSOR_ACCEPT,
        SUCCESSOR_ACCEPT_READ,
        SUCCESSOR_ACTION_WRITE,
        SUCCESSOR_HEARTBEAT_WRITE
    }
}

class RingChannelImpl(override val type: RingChannel.Type) : RingChannel
