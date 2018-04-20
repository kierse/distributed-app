package eece513.server

import eece513.LOCATE_CMD
import eece513.LS_CMD
import eece513.common.Logger
import eece513.common.FILE_SYSTEM_SEPARATOR
import eece513.common.util.getLatestVersion
import eece513.common.util.getLocalFiles
import eece513.common.util.unescapeFileName

/**
 * Wrap UNIX grep in the [GrepServer.QueryService] interface. This class
 * pipes grep output (both stdout and stderr) to the given result handlers
 */
class GrepQueryService(
        private val logger: Logger
) : GrepServer.QueryService {
    private val tag = GrepQueryService::class.java.simpleName

    /**
     * Run grep passing the given list of args and returning stdout to [onResult] and
     * stderr to [onError].
     */
    override fun search(
            args: Array<String>, onResult: (String) -> Unit, onError: (Array<String>) -> Unit
    ) {
        val (instruction: String, instructionArg: String) = args.first().split(":")

        val results = when (instruction) {
            LS_CMD -> {
                getLocalFiles().map { file ->
                    unescapeFileName(file.name.substringBeforeLast(FILE_SYSTEM_SEPARATOR))
                }
            }

            LOCATE_CMD -> {
                listOf(if (getLatestVersion(instructionArg) == null) "0" else "1")
            }

            else -> throw IllegalArgumentException("unknown/unrecognized instruction: $instruction")
        }

        results.forEach { result ->
            onResult.invoke(result)
        }

    }
}