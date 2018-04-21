package eece513.client

import eece513.common.Logger
import java.io.PrintStream

/**
 * This class processes [Client.Server.Response]'s and prints them
 * to the given [out] and [err] PrintStream's. Instances of this class
 * can be used to print results out to the console.
 */
class PrintStreamPresenter(
        private val out: PrintStream, private val err: PrintStream, private val logger: Logger
) : Client.Presenter {
    override fun displayResponse(response: Client.Server.Response) {
        when (response) {
            is Client.Server.Response.Result -> printStdOut(response)
            is Client.Server.Response.Error -> printStdErr(response)
        }
    }

    override fun displayHelp(msg: String) = err.println(msg)

    private fun printStdOut(response: Client.Server.Response.Result) {
        val prefix = if (response.name.isEmpty()) "" else "${response.name}:"
        for (line in response.result) {
            out.println("$prefix$line")
        }
    }

    private fun printStdErr(response: Client.Server.Response.Error) {
        val prefix = if (response.name.isEmpty()) "" else "${response.name}:"
        for (line in response.result) {
            err.println("$prefix$line")
        }
    }
}