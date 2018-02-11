package eece513

import eece513.model.MembershipList
import eece513.model.Node
import java.net.InetSocketAddress
import java.nio.channels.ReadableByteChannel
import java.time.Instant

class MembershipListFactory(private val messageReader: MessageReader) {
    fun build(channel: ReadableByteChannel): MembershipList? {
        val bytes = messageReader.read(channel)
        if (bytes.isEmpty()) return null

        val parsed = Actions.MembershipList.parseFrom(bytes)

        val nodes = mutableListOf<Node>()
        for (membership in parsed.nodeList) {
            val addr = InetSocketAddress(membership.hostName, membership.port)
            val timestamp = Instant.ofEpochSecond(
                    membership.timestamp.secondsSinceEpoch,
                    membership.timestamp.nanoSeconds.toLong()
            )
            nodes.add(Node(addr, timestamp))
        }

        return MembershipList(nodes)
    }
}