package eece513.server

import eece513.*
import eece513.common.Logger
import eece513.common.TinyLogWrapper

/**
 * Instances of this class can be used to respond to searches from client.
 */
class GrepServer(
        private val queryService: QueryService,
        private val connectionListener: ConnectionListener,
        private val logger: Logger
) {
    interface QueryService {
        fun search(args: Array<String>, onResult: (String) -> Unit, onError: (Array<String>) -> Unit)
    }

    interface ConnectionListener {
        interface Connection {
            fun getQueryArgs(): Array<String>
            fun sendResult(result: String)
            fun sendError(error: Array<String>)
        }

        fun listen(onQuery: (Connection) -> Unit)
    }

    companion object {
        private val tag: String = GrepServer::class.java.simpleName

        @JvmStatic
        fun main(args: Array<String>) {
            val logger = TinyLogWrapper(SERVER_LOG_LOCATION)
            val queryService = GrepQueryService(logger)
            val queryListener = SocketConnectionListener(SERVER_PORT, logger)

            GrepServer(queryService, queryListener, logger)
                    .run()
        }
    }

    /**
     * Invoking this method starts the server allowing it to begin listening for
     * remote search connections.
     */
    internal fun run() {
        logger.info(tag, "booting up...")

        // start listening for remote connection
        //
        // Note: this method listens for incoming connections. It is capable of handling
        // one connection at a time, however, it will start listening for new connections
        // when on closes.
        connectionListener.listen { connection ->
            val args = connection.getQueryArgs()
            logger.debug(tag, "query args: [{}]", args.joinToString(", "))

            // on connection, query the local grep service and channel results
            // back to connection
            queryService.search(
                    args,
                    { connection.sendResult(it) },
                    { connection.sendError(it) }
            )
        }
    }
}