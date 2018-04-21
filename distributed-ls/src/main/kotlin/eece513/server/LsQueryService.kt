package eece513.server

import eece513.common.Logger
import eece513.common.util.getLocalFiles

class LsQueryService(private val logger: Logger) : Server.QueryService {
    private val tag = LsQueryService::class.java.simpleName

    override fun execute(
            args: Array<String>, onResult: (String) -> Unit, onError: (Array<String>) -> Unit
    ) {
        getLocalFiles()
                .forEach { result ->
                    onResult.invoke(result)
                }
    }
}