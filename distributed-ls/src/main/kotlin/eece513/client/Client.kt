package eece513.client

import eece513.*
import eece513.common.*
import eece513.common.util.FileIO
import eece513.common.util.getLocalFiles
import eece513.common.util.unescapeFileName
import java.util.concurrent.ConcurrentLinkedQueue


class Client(
        private val presenter: Presenter,
        private val helpGenerator: HelpGenerator,
        private val logger: Logger,
        private val servers: List<Server>
) {
    interface Presenter {
        fun displayResponse(response: Server.Response)
        fun displayHelp(msg: String)
    }

    interface Server {
        sealed class Response {
            abstract val name: String
            abstract val result: List<String>
            
            data class Result(override val name: String, override val result: List<String>) : Response()
            data class Error(override val name: String, override val result: List<String>) : Response()
        }

        interface Query {
            fun isComplete(): Boolean
        }

        val name: String

        fun execute(args: Array<String>, onResult: (Response) -> Unit): Query
    }

    interface HelpGenerator {
        fun getHelpMessage(): String
    }

    companion object {
        private val tag: String = Client::class.java.simpleName

        @JvmStatic
        fun main(args: Array<String>) {
            val logger = TinyLogWrapper(CLIENT_LOG_LOCATION)
            val presenter = PrintStreamPresenter(System.out, System.err, logger)
            val helpGenerator = GrepHelpGenerator(GREP_CMD, logger)

            val servers = FileIO().ReadLinesAsInetAddress(SERVERS_FILE_PATH).map { address ->
                ServerImpl(address, SERVER_PORT, address.hostAddress, logger)
            }

            Client(presenter, helpGenerator, logger, servers)
                    .execute(args)
        }
    }

    init {
        if (servers.isEmpty()) throw IllegalArgumentException("must provide at least one server!")
    }

    fun execute(args: Array<String>) {
        logger.info(tag, "args: [{}]", args.joinToString(", "))

        when (args.firstOrNull()) {
            LSHERE_CMD -> getLocalFiles().forEach(::println)

            LOCATE_CMD -> processQueries(servers, args) { response ->
                if (response.result.first() == "1") Client.Server.Response.Result("", listOf(response.name))
                else null
            }

            LS_CMD -> processQueries(servers.subList(0, 1), arrayOf(LS_CMD, "")) { response ->
                Client.Server.Response.Result("", response.result)
            }

            GREP_CMD -> processQueries(servers, args)

            else -> presenter.displayHelp(helpGenerator.getHelpMessage())
        }
    }

    private fun processQueries(
            servers: List<Server>,
            args: Array<String>,
            resultMapper: (Server.Response) -> Server.Response? = { it }
    ) {
        val queue: ConcurrentLinkedQueue<Server.Response> = ConcurrentLinkedQueue()

        var queries = servers.map { server ->
            logger.debug(tag, "searching ${server.name}...")
            server.execute(args) { response ->
                logger.debug(tag, "response: {}", response)

                resultMapper(response)
                        ?.let { queue.add(it) }
            }
        }

        while (queries.isNotEmpty() || queue.isNotEmpty()) {
            while (true) {
                val result = queue.poll() ?: break
                logger.debug(tag, "printing: $result")
                presenter.displayResponse(result)
            }
            queries = filterQueries(queries)
        }
    }

    private fun filterQueries(queries: List<Server.Query>): List<Server.Query> {
        return queries.filterNot { query ->
            query.isComplete()
        }
    }
}
