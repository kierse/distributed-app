package eece513.server

import eece513.common.Logger
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.Socket

/**
 * This class wraps a [java.net.Socket] and implements methods that expose
 * input ([getQueryArgs]) and output ([sendResult] / [sendError])
 */
class ConnectionImpl(socket: Socket, private val logger: Logger) : GrepServer.ConnectionListener.Connection {
    private val tag = ConnectionImpl::class.java.simpleName

    private val br: BufferedReader
    private val bw: BufferedWriter

    init {
        val isr = InputStreamReader(socket.getInputStream())
        br = BufferedReader(isr)

        val osw = OutputStreamWriter(socket.getOutputStream())
        bw = BufferedWriter(osw)
    }

    override fun getQueryArgs(): Array<String> {
        val args = mutableListOf<String>()
        while (true) {
            val arg = br.readLine() ?: break
            logger.debug(tag, "arg: $arg")
            args.add(arg)
        }

        logger.debug(tag, "received ${args.size} args...")

        return args.toTypedArray()
    }

    /**
     * This method writes the given result to the socket output stream.
     *
     * Because stdout and stderr are both sent over the same socket stream,
     * all messages are preceded by a header consisting of a single character
     * (R for result) and the size of the message (in number of lines)
     */
    override fun sendResult(result: String) {
        logger.debug(tag, "sending result: {}", result)
        bw.write("R:1\n")
        bw.write("$result\n")
        bw.flush()
    }

    /**
     * This method writes the given error to the socket output stream.
     *
     * Because stdout and stderr are both sent over the same socket stream,
     * all messages are preceded by a header consisting of a single character
     * (E for error) and the size of the message (in number of lines)
     */
    override fun sendError(error: Array<String>) {
        bw.write("E:${error.size}\n")
        for (line in error) {
            logger.error(tag, "error: {}", line)
            bw.write("$line\n")
        }

        bw.flush()
    }
}