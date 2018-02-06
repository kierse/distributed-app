package eece513.model

import java.net.SocketAddress
import java.time.Instant

data class Node(val addr: SocketAddress, val joinedAt: Instant)