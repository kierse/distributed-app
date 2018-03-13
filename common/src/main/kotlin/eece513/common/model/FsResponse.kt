package eece513.common.model

sealed class FsResponse {
    interface PutResponse {
        val pathToFile: String
    }

    class RemoveResponse : FsResponse()

    data class GetResponse(val pathToFile: String) : FsResponse()

    data class PutAck(override val pathToFile: String) : PutResponse, FsResponse()
    data class PutAckWithPrompt(override val pathToFile: String) : PutResponse, FsResponse()

    class UnknownFile : FsResponse()
}
