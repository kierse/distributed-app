package eece513.client

import eece513.Logger
import java.net.InetAddress

/**
 * ServerImpl implements [GrepClient.Server] and is responsible for performing searches on a background thread.
 */
class ServerImpl(
        private val ip: InetAddress,
        private val port: Int,
        override val name: String,
        private val logger: Logger
) : GrepClient.Server {

    /**
     * Start search on background thread and return immediately.
     *
     * Note: the given result handler ([onResult]) will be invoked on the background thread!
     */
    override fun search(args: Array<String>, onResult: (GrepClient.Server.Response) -> Unit): GrepClient.Server.Query {
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