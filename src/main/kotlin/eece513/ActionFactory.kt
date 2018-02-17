package eece513

import eece513.model.Action
import eece513.model.Node
import java.net.InetSocketAddress
import java.nio.channels.DatagramChannel
import java.nio.channels.ReadableByteChannel
import java.time.Instant

class ActionFactory(private val messageReader: MessageReader) {
    fun buildList(channel: ReadableByteChannel): List<Action> {
        val actions = mutableListOf<Action>()

        while (true) {
            actions.add(build(channel) ?: break)
        }

        return actions
    }

    fun build(readableChannel: ReadableByteChannel): Action? {
        val bytes = messageReader.read(readableChannel)
        if (bytes.isEmpty()) return null

        return buildAction(bytes)
    }

    fun build(datagramChannel: DatagramChannel): Action? {
        val bytes = messageReader.read(datagramChannel)
        if (bytes.isEmpty()) return null

        return buildAction(bytes)
    }

    private fun buildAction(bytes: ByteArray): Action {
        val parsed = Actions.Request.parseFrom(bytes)

        val address = InetSocketAddress(parsed.hostName, parsed.port)
        val instant = Instant.ofEpochSecond(
                parsed.timestamp.secondsSinceEpoch, parsed.timestamp.nanoSeconds.toLong()
        )
        val node = Node(address, instant)

        return when (parsed.type) {
            Actions.Request.Type.JOIN -> Action.Join(node)
            Actions.Request.Type.DROP -> Action.Drop(node)
            Actions.Request.Type.CONNECT -> Action.Connect(node)
            Actions.Request.Type.HEARTBEAT -> Action.Heartbeat(node)

            else -> throw IllegalArgumentException("unrecognized type! ${parsed.type.name}")
        }
    }
}