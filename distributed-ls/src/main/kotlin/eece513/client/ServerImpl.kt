package eece513.client

import eece513.common.Logger
import java.net.InetAddress

/**
 * ServerImpl implements [Client.Server] and is responsible for performing searches on a background thread.
 */
class ServerImpl(
        private val ip: InetAddress,
        private val port: Int,
        override val name: String,
        private val logger: Logger
) : Client.Server {

    /**
     * Start execute on background thread and return immediately.
     *
     * Note: the given result handler ([onResult]) will be invoked on the background thread!
     */
    override fun execute(args: Array<String>, onResult: (Client.Server.Response) -> Unit): Client.Server.Query {
        val serverQuery = QueryImpl(
                ip = ip,
                port = port,
                id = name,
                args = args,
                logger = logger,
                onResponse = onResult
        )

        Thread(serverQuery).start()

        return serverQuery
    }
}