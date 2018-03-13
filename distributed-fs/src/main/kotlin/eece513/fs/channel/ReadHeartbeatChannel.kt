package eece513.fs.channel

import eece513.fs.DATAGRAM_PACKET_LIMIT
import eece513.common.Logger
import eece513.common.mapper.EmptyByteArrayException
import eece513.common.mapper.ObjectMapper
import eece513.common.mapper.ParseException
import eece513.common.model.Action
import eece513.common.model.Node
import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel

class ReadHeartbeatChannel(
        private val channel: DatagramChannel,
        private val actionMapper: ObjectMapper<Action.ClusterAction>,
        private val logger: Logger
) : RingChannel {
    private val tag = ReadHeartbeatChannel::class.java.simpleName
    override val type = RingChannel.Type.PREDECESSOR_HEARTBEAT_READ

    fun read(): Node? {
        val packetBuffer = ByteBuffer.allocate(DATAGRAM_PACKET_LIMIT)
        val source = channel.receive(packetBuffer)

        val length = packetBuffer.position()
        packetBuffer.flip()

        val bodyBuffer = ByteArray(length)
        packetBuffer.get(bodyBuffer)

        val action = try {
            logger.debug(tag, "received $length byte heartbeat from $source")
            actionMapper.toObject(bodyBuffer)
        } catch (exception: ParseException) {
            logger.error(tag, "Error mapping heartbeat action: %s", exception)
            return null
        } catch (exception: EmptyByteArrayException) {
            logger.error(tag, "Message body empty! %s", exception)
            return null
        }

        if (action !is Action.ClusterAction.Heartbeat) {
            logger.error(tag, "expected Action.Heartbeat, received: ${action.javaClass.simpleName}")
            return null
        }

        return action.node
    }
}