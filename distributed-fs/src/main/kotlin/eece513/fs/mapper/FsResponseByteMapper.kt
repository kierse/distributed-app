package eece513.fs.mapper

import eece513.common.Fs
import eece513.common.mapper.ByteMapper
import eece513.common.model.FsResponse

class FsResponseByteMapper : ByteMapper<FsResponse> {
    override fun toByteArray(obj: FsResponse): ByteArray {
        val (type, body) = when (obj) {
            is FsResponse.UnknownFile -> {
                Pair(Fs.Response.Type.UNKNOWN_FILE, "")
            }

            is FsResponse.RemoveResponse -> {
                Pair(Fs.Response.Type.REMOVE, "")
            }

            is FsResponse.PutAckWithPrompt -> {
                Pair(Fs.Response.Type.PUT_ACK_PROMPT, obj.pathToFile)
            }

            is FsResponse.PutAck -> {
                Pair(Fs.Response.Type.PUT_ACK, obj.pathToFile)
            }

            is FsResponse.GetResponse -> {
                Pair(Fs.Response.Type.GET, obj.pathToFile)
            }
        }

        return Fs.Response.newBuilder()
                .setType(type)
                .setBody(body)
                .build()
                .toByteArray()
    }
}