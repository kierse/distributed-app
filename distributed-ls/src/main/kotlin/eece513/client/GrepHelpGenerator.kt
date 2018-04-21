package eece513.client

import eece513.common.Logger
import java.io.BufferedReader
import java.io.InputStreamReader

/**
 * Instances of this class run `grep --help` and return the results
 */
class GrepHelpGenerator(private val cmd: String, private val logger: Logger) : Client.HelpGenerator {
    private val tag = GrepHelpGenerator::class.java.simpleName

    override fun getHelpMessage(): String {
        var esr: InputStreamReader? = null
        var ebr: BufferedReader? = null

        val help = mutableListOf<String>()
        try {
            val proc = ProcessBuilder(cmd, "--help").start()

            // Note: we're grabbing result from stderr NOT stdin
            esr = InputStreamReader(proc.errorStream)
            ebr = BufferedReader(esr)

            while (true) {
                val line = ebr.readLine() ?: break
                help.add(line)
            }
        } finally {
            esr?.close()
            ebr?.close()
        }

        return help.joinToString("\n")
    }
}