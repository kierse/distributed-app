package eece513.fs.mapper

import com.google.protobuf.ByteString
import eece513.common.mapper.ByteMapper
import eece513.common.mapper.ObjectMapper
import eece513.common.model.Action
import eece513.fs.Actions

class ActionMapper(
        private val clusterActionMapper: ClusterActionMapper,
        private val fileSystemActionMapper: FileSystemMapper
) : ObjectMapper<Action>, ByteMapper<Action> {
    override fun toObjectOrNull(byteArray: ByteArray): Action? {
        val envelope = Actions.Envelope.parseFrom(byteArray) ?: return null

        val body = envelope.action.toByteArray()
        return when (envelope.type) {
            Actions.Envelope.Type.CLUSTER_ACTION -> clusterActionMapper.toObject(body)
            Actions.Envelope.Type.FILE_SYSTEM_ACTION -> fileSystemActionMapper.toObject(body)

            else -> throw IllegalArgumentException("unknown envelope type!")
        }
    }

    override fun toByteArray(obj: Action): ByteArray {
        val (type, body) = when (obj) {
            is Action.ClusterAction -> {
                Pair(Actions.Envelope.Type.CLUSTER_ACTION, clusterActionMapper.toByteArray(obj))
            }
            is Action.FileSystemAction -> {
                Pair(Actions.Envelope.Type.FILE_SYSTEM_ACTION, fileSystemActionMapper.toByteArray(obj))
            }

            else -> throw IllegalArgumentException("unknown action type!")
        }

        return Actions.Envelope.newBuilder()
                .setType(type)
                .setAction(ByteString.copyFrom(body))
                .build()
                .toByteArray()
    }
}