package eece513.fs.channel

interface RingChannel {
    val type: Type

    enum class Type {
        JOIN_ACCEPT,
        JOIN_ACCEPT_READ,
        JOIN_ACCEPT_WRITE,
        JOIN_CONNECT,
        JOIN_CONNECT_WRITE,
        JOIN_CONNECT_READ,
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
