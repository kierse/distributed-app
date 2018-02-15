package eece513

import eece513.model.Node
import java.net.InetSocketAddress
import java.nio.channels.ReadableByteChannel
import java.time.Instant

class NodeFactory(private val messageReader: MessageReader) {
    fun buildList(channel: ReadableByteChannel): List<Node> {
        val nodes = mutableListOf<Node>()

        while (true) {
            val bytes = messageReader.read(channel)
            if (bytes.isEmpty()) break

            val parsed = Actions.Membership.parseFrom(bytes)

            val address = InetSocketAddress(parsed.hostName, parsed.port)
            val instant = Instant.ofEpochSecond(
                    parsed.timestamp.secondsSinceEpoch, parsed.timestamp.nanoSeconds.toLong()
            )

            nodes.add(Node(address, instant))
        }

        return nodes
    }
}