package eece513.server

import eece513.LOCATE_CMD
import eece513.Logger
import java.io.BufferedReader
import java.io.InputStreamReader

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
        val argsList = arrayOf(*args)
        logger.debug(tag, "grep cmd: {}", argsList.joinToString(" "))

        var esr: InputStreamReader? = null
        var isr: InputStreamReader? = null

        var ebr: BufferedReader? = null
        var ibr: BufferedReader? = null

        try {
            val proc = ProcessBuilder(*argsList).start()
            var responseInProgress = false

            esr = InputStreamReader(proc.errorStream)
            ebr = BufferedReader(esr)

            isr = InputStreamReader(proc.inputStream)
            ibr = BufferedReader(isr)


            if (ebr.ready()) {
                // when "locate" doesn't find match, it returns 1
                if (ebr.readLine() != "1" && args.first() != LOCATE_CMD) {
                    onError.invoke(ebr.readLines().toTypedArray())
                    return
                }
            }

            val result = mutableListOf<String>()
            while (true) {
                val line = ibr.readLine() ?: break
                responseInProgress = true
                result.add(line)
            }
            if (responseInProgress) {
                responseInProgress = false
                onResult.invoke(result.joinToString(", "))
            }

            logger.debug(tag, "no more results!")
        } finally {
            esr?.close()
            isr?.close()

            ebr?.close()
            ibr?.close()
        }
    }
}