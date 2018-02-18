package eece513

const val PORT = 6970
const val JOIN_PORT = 6971

const val MESSAGE_HEADER_SIZE = 2 // in bytes

const val DATAGRAM_PACKET_LIMIT = 128 // in bytes

const val RELATIONSHIP_COUNT = 3

const val HEARTBEAT_INTERVAL = 1700L
const val HEARTBEAT_TIMEOUT = 1900L

const val LOG_LOCATION = "logs/distributed-app.log"

const val SUCCESSOR_SENT_ACTIONS_LIMIT = 100