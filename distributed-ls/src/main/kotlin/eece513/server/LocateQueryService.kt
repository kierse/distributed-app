package eece513.server

import eece513.common.Logger
import eece513.common.util.getLatestVersion

class LocateQueryService(private val logger: Logger) : Server.QueryService {
    private val tag = LocateQueryService::class.java.simpleName

    override fun execute(
            args: Array<String>, onResult: (String) -> Unit, onError: (Array<String>) -> Unit
    ) {
        onResult.invoke(if (getLatestVersion(args.first()) == null) "0" else "1")
    }
}