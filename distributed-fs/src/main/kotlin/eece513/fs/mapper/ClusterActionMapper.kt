package eece513.fs.mapper

import com.google.protobuf.InvalidProtocolBufferException
import eece513.common.mapper.ByteMapper
import eece513.common.mapper.EmptyByteArrayException
import eece513.common.mapper.ObjectMapper
import eece513.common.model.Action
import eece513.fs.Actions
import eece513.common.model.Node
import java.net.InetSocketAddress
import java.time.Instant

class ClusterActionMapper : ObjectMapper<Action.ClusterAction>, ByteMapper<Action.ClusterAction> {
    override fun toObjectOrNull(byteArray: ByteArray): Action.ClusterAction? {
        if (byteArray.isEmpty()) {
            throw EmptyByteArrayException("byteArray can't be empty!")
        }

        val parsed: Actions.Request
        try {
            parsed = Actions.Request.parseFrom(byteArray)
        } catch (e: InvalidProtocolBufferException) {
            return null
        }

        val address = InetSocketAddress(parsed.hostName, parsed.port)
        val instant = Instant.ofEpochSecond(
                parsed.timestamp.secondsSinceEpoch, parsed.timestamp.nanoSeconds.toLong()
        )
        val node = Node(address, instant)

        return when (parsed.type) {
            Actions.Request.Type.JOIN -> Action.ClusterAction.Join(node)
            Actions.Request.Type.REMOVE -> Action.ClusterAction.Leave(node)
            Actions.Request.Type.DROP -> Action.ClusterAction.Drop(node)
            Actions.Request.Type.CONNECT -> Action.ClusterAction.Connect(node)
            Actions.Request.Type.HEARTBEAT -> Action.ClusterAction.Heartbeat(node)

            else -> throw IllegalArgumentException("unrecognized type! ${parsed.type.name}")
        }
    }

    override fun toByteArray(obj: Action.ClusterAction): ByteArray {
        val type = when (obj) {
            is Action.ClusterAction.Join -> Actions.Request.Type.JOIN
            is Action.ClusterAction.Leave -> Actions.Request.Type.REMOVE
            is Action.ClusterAction.Drop -> Actions.Request.Type.DROP
            is Action.ClusterAction.Connect -> Actions.Request.Type.CONNECT
            is Action.ClusterAction.Heartbeat -> Actions.Request.Type.HEARTBEAT
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