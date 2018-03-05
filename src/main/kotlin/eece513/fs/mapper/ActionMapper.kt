package eece513.fs.mapper

import com.google.protobuf.InvalidProtocolBufferException
import eece513.fs.Actions
import eece513.fs.model.Action
import eece513.fs.model.Node
import java.net.InetSocketAddress
import java.time.Instant

class ActionMapper : ObjectMapper<Action> {
    override fun toObject(byteArray: ByteArray): Action {
        if (byteArray.isEmpty()) {
            throw ObjectMapper.EmptyByteArrayException("byteArray can't be empty!")
        }

        val parsed: Actions.Request
        try {
            parsed = Actions.Request.parseFrom(byteArray)
        } catch (e: InvalidProtocolBufferException) {
            throw ObjectMapper.ParseException(e)
        }

        val address = InetSocketAddress(parsed.hostName, parsed.port)
        val instant = Instant.ofEpochSecond(
                parsed.timestamp.secondsSinceEpoch, parsed.timestamp.nanoSeconds.toLong()
        )
        val node = Node(address, instant)

        return when (parsed.type) {
            Actions.Request.Type.JOIN -> Action.Join(node)
            Actions.Request.Type.REMOVE -> Action.Leave(node)
            Actions.Request.Type.DROP -> Action.Drop(node)
            Actions.Request.Type.CONNECT -> Action.Connect(node)
            Actions.Request.Type.HEARTBEAT -> Action.Heartbeat(node)

            else -> throw IllegalArgumentException("unrecognized type! ${parsed.type.name}")
        }
    }

    override fun toByteArray(obj: Action): ByteArray {
        val type = when (obj) {
            is Action.Join -> Actions.Request.Type.JOIN
            is Action.Leave -> Actions.Request.Type.REMOVE
            is Action.Drop -> Actions.Request.Type.DROP
            is Action.Connect -> Actions.Request.Type.CONNECT
            is Action.Heartbeat -> Actions.Request.Type.HEARTBEAT
        }

        val time = obj.node.joinedAt
        val timestamp = Actions.Timestamp.newBuilder()
                .setSecondsSinceEpoch(time.epochSecond)
                .setNanoSeconds(time.nano)
                .build()

        val address = obj.node.addr
        return Actions.Request.newBuilder()
                .setType(type)
                .setTimestamp(timestamp)
                .setHostName(address.hostName)
                .setPort(address.port)
                .build()
                .toByteArray()
    }
}