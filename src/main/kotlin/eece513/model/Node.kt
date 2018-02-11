package eece513.model

import java.net.InetSocketAddress
import java.time.Instant

data class Node(val addr: InetSocketAddress, val joinedAt: Instant)