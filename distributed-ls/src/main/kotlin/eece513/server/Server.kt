package eece513.server

import eece513.*
import eece513.common.FS_LOG_LOCATION
import eece513.common.Logger
import eece513.common.TinyLogWrapper

/**
 * Instances of this class can be used to respond to searches from client.
 */
class Server(
        private val grepService: QueryService,
        private val lsService: QueryService,
        private val locateService: QueryService,
        private val connectionListener: ConnectionListener,
        private val logger: Logger
) {
    interface QueryService {
        fun execute(args: Array<String>, onResult: (String) -> Unit, onError: (Array<String>) -> Unit)
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
        private val tag: String = Server::class.java.simpleName

        @JvmStatic
        fun main(args: Array<String>) {
            val logger = TinyLogWrapper(SERVER_LOG_LOCATION)

            val grepService = GrepQueryService(GREP_CMD, FS_LOG_LOCATION, logger)
            val lsService = LsQueryService(logger)
            val locateService = LocateQueryService(logger)

            val queryListener = SocketConnectionListener(SERVER_PORT, logger)

            Server(grepService, lsService, locateService, queryListener, logger)
                    .run()
        }
    }

    /**
     * Invoking this method starts the server allowing it to begin listening for
     * remote execute connections.
     */
    internal fun run() {
        logger.info(tag, "booting up...")

        // start listening for remote connection
        //
        // Note: this method listens for incoming connections. It is capable of handling
        // one connection at a time, however, it will start listening for new connections
        // when on closes.
        connectionListener.listen(
                fun (connection) {
                    val args = connection.getQueryArgs()
                    logger.debug(tag, "query args: [{}]", args.joinToString(", "))

                    val service = when (args.getOrNull(0)) {
                        GREP_CMD ->  grepService
                        LS_CMD -> lsService
                        LOCATE_CMD -> locateService

                        else -> null
                    }

                    if (service == null) {
                        if (args.isEmpty()) {
                            connection.sendError(arrayOf("must provide an instruction!"))
                        } else {
                            connection.sendError(arrayOf("unknown/unsupported service type: ${args.first()}"))
                        }

                        return
                    }

                    // on connection, query the local grep service and channel results
                    // back to connection
                    service.execute(
                            args.copyOfRange(1, args.size),
                            { connection.sendResult(it) },
                            { connection.sendError(it) }
                    )
                }
        )
    }
}