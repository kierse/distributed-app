package eece513

import eece513.model.Action
import eece513.model.Node
import java.net.InetSocketAddress
import java.nio.channels.ReadableByteChannel
import java.time.Instant

class ActionFactory(
        private val messageReader: MessageReader,
        private val logger: Logger
) {
    fun build(channel: ReadableByteChannel): List<Action> {
        val actions = mutableListOf<Action>()

        do {
            val bytes = messageReader.read(channel)
            if (bytes.isEmpty()) break

            val parsed = Actions.Request.parseFrom(bytes)

            val addr = InetSocketAddress(parsed.hostName, parsed.port)
            val instant = Instant.ofEpochSecond(
                    parsed.timestamp.secondsSinceEpoch, parsed.timestamp.nanoSeconds.toLong()
            )
            val node = Node(addr, instant)

            val action: Action = when (parsed.type) {
                Actions.Request.Type.JOIN -> Action.Join(node)
                Actions.Request.Type.REMOVE -> Action.Leave(node)
                Actions.Request.Type.DROP -> Action.Drop(node)

                else -> throw IllegalArgumentException("unrecognized type! ${parsed.type.name}")
            }

            actions.add(action)
        } while (true)

        logger.info("ActionFactory", "created ${actions.size} actions")
        return actions
    }
}