package eece513.common.mapper

import com.google.protobuf.InvalidProtocolBufferException
import eece513.common.Connection
import eece513.common.model.ConnectionPurpose

class ConnectionPurposeMapper : ObjectMapper<ConnectionPurpose>, ByteMapper<ConnectionPurpose> {
    override fun toByteArray(obj: ConnectionPurpose): ByteArray {
        val type = when (obj) {
            is ConnectionPurpose.NodeJoin -> Connection.Purpose.Type.JOIN
            is ConnectionPurpose.ClientRemove -> Connection.Purpose.Type.REMOVE
            is ConnectionPurpose.ClientGet -> Connection.Purpose.Type.GET
            is ConnectionPurpose.ClientPut -> Connection.Purpose.Type.PUT
        }

        return Connection.Purpose.newBuilder()
                .setType(type)
                .build()
                .toByteArray()
    }

    override fun toObjectOrNull(byteArray: ByteArray): ConnectionPurpose? {
        if (byteArray.isEmpty()) {
            throw EmptyByteArrayException("byteArray can't be empty!")
        }

        val parsed: Connection.Purpose
        try {
            parsed = Connection.Purpose.parseFrom(byteArray)
        } catch (e: InvalidProtocolBufferException) {
            return null
        }

        return when (parsed.type) {
            Connection.Purpose.Type.GET -> ConnectionPurpose.ClientGet()
            Connection.Purpose.Type.PUT -> ConnectionPurpose.ClientPut()
            Connection.Purpose.Type.REMOVE -> ConnectionPurpose.ClientRemove()
            Connection.Purpose.Type.JOIN -> ConnectionPurpose.NodeJoin()

            else -> throw IllegalArgumentException("unrecognized type! ${parsed.type.name}")
        }
    }
}