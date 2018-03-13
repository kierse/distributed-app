package eece513.fsClient.mapper

import com.google.protobuf.InvalidProtocolBufferException
import eece513.common.Fs
import eece513.common.mapper.EmptyByteArrayException
import eece513.common.mapper.ObjectMapper
import eece513.common.model.FsResponse

class FsResponseObjectMapper : ObjectMapper<FsResponse> {
    override fun toObjectOrNull(byteArray: ByteArray): FsResponse? {
        if (byteArray.isEmpty()) {
            throw EmptyByteArrayException("byteArray can't be empty!")
        }

        val parsed: Fs.Response
        try {
            parsed = Fs.Response.parseFrom(byteArray)
        } catch (e: InvalidProtocolBufferException) {
            return null
        }

        return when (parsed.type) {
            Fs.Response.Type.GET -> FsResponse.GetResponse(parsed.body)
            Fs.Response.Type.PUT_ACK-> FsResponse.PutAck(parsed.body)
            Fs.Response.Type.PUT_ACK_PROMPT -> FsResponse.PutAckWithPrompt(parsed.body)
            Fs.Response.Type.REMOVE -> FsResponse.RemoveResponse()
            Fs.Response.Type.UNKNOWN_FILE -> FsResponse.UnknownFile()

            else -> throw IllegalArgumentException("unrecognized type! ${parsed.type.name}")
        }
    }
}