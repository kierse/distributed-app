package eece513.client

import eece513.common.Logger
import java.io.PrintStream

/**
 * This class processes [GrepClient.Server.Response]'s and prints them
 * to the given [out] and [err] PrintStream's. Instances of this class
 * can be used to print results out to the console.
 */
class PrintStreamPresenter(
        private val out: PrintStream, private val err: PrintStream, private val logger: Logger
) : GrepClient.Presenter {
    override fun displayResponse(response: GrepClient.Server.Response) {
        when (response) {
            is GrepClient.Server.Response.Result -> printStdOut(response)
            is GrepClient.Server.Response.Error -> printStdErr(response)
        }
    }

    override fun displayHelp(msg: String) = err.println(msg)

    private fun printStdOut(response: GrepClient.Server.Response.Result) {
        for (line in response.result) {
            out.println(line)
        }
    }

    private fun printStdErr(response: GrepClient.Server.Response.Error) {
        for (line in response.result) {
            err.println("${response.name}:$line")
        }
    }
}