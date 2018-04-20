package eece513.client

import eece513.common.Logger
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.*
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Class that implements [GrepClient.Server.Query] and uses java.net.Socket's internally. This class opens a Java
 * Socket to the given [ip] and [port], then marshall's data between the remote server and the invoking code.
 *
 * Note: QueryImpl implements the Runnable interface so instances can be executed by [Thread].
 */
class QueryImpl(
        private val ip: InetAddress,
        private val port: Int,
        private val id: String,
        private val args: Array<String>,
        private val logger: Logger,
        private val onResponse: (GrepClient.Server.Response) -> Unit
) : GrepClient.Server.Query, Runnable {

    private val tag = QueryImpl::class.java.simpleName
    private val moreResults = AtomicBoolean(true)

    override fun run() = run(Socket())

    internal fun run(socket: Socket) {
        try {
            socket.connect(InetSocketAddress(ip, port), 1000) // timeout after 1 second
            process(socket)
        } catch (e: ConnectException) {
            handleConnectionError()
        } catch (e: SocketTimeoutException) {
            handleConnectionError()
        } finally {
            socket.close()
        }
    }

    private fun process(socket: Socket) {
        val osw = OutputStreamWriter(socket.getOutputStream())
        val bw = BufferedWriter(osw)

        bw.write(args.joinToString(":"))
        bw.flush()

        // indicate no more data
        socket.shutdownOutput()

        val isr = InputStreamReader(socket.getInputStream())
        val br = BufferedReader(isr)

        while (true) {
            val header = br.readLine() ?: break

            val (type, count) = header.split(":")
            val result = readValue(br, count.toInt())

            val response = when (type) {
                "E" -> GrepClient.Server.Response.Error(
                        name = id, result = result
                )

                "R" -> GrepClient.Server.Response.Result(
                        name = id, result = result
                )

                else -> throw IllegalArgumentException("unknown header type: $type")
            }

            onResponse.invoke(response)
        }

        socket.shutdownInput()
        moreResults.set(false)
    }

    private fun handleConnectionError() {
        val response = GrepClient.Server.Response.Error(
                name = id, result = listOf("Server Unreachable")
        )
        onResponse.invoke(response)
        moreResults.set(false)
    }

    private fun readValue(reader: BufferedReader, count: Int): List<String> {
        val list = ArrayList<String>(count)
        (0 until count).forEach {
            list.add(reader.readLine())
        }

        return list
    }

    override fun isComplete(): Boolean = !moreResults.get()
}