package eece513.client

import eece513.*
import eece513.server.GrepServer
import java.util.concurrent.ConcurrentLinkedQueue
import java.io.BufferedReader
import java.io.InputStreamReader


class GrepClient(
        private val presenter: Presenter,
        private val helpGenerator: HelpGenerator,
        private val logger: Logger,
        private val servers: List<Server>
) {
    interface Presenter {
        fun displayResponse(response: Server.Response)
        fun displayMachinesOnly(response: Server.Response)
        fun displayHelp(msg: String)
    }

    interface Server {
        sealed class Response {
            data class Result(val name: String, val result: List<String>) : Response()
            data class Error(val name: String, val result: List<String>) : Response()
        }

        interface Query {
            fun isComplete(): Boolean
        }

        val name: String

        fun search(args: Array<String>, onResult: (Response) -> Unit): Query
    }

    interface HelpGenerator {
        fun getHelpMessage(): String
    }

    companion object {
        private val tag: String = GrepClient::class.java.simpleName

        @JvmStatic
        fun main(args: Array<String>) {
            val presenter = PrintStreamPresenter(System.out, System.err)
            val logger = TinyLogWrapper(CLIENT_LOG_LOCATION)
            val helpGenerator = GrepHelpGenerator(GREP_CMD, logger)

            val servers = FileIO().ReadLinesAsInetAddress(System.getProperty("user.dir") + "/servers.txt").map { address ->
                ServerImpl(address, SERVER_PORT, address.hostAddress, logger)
            }

            GrepClient(presenter, helpGenerator, logger, servers)
                    .search(args)
        }
    }

    init {
        if (servers.isEmpty()) throw IllegalArgumentException("must provide at least one server!")
    }

    fun search(args: Array<String>) {
        if (args.first() == LSHERE_CMD) {
            val proc = ProcessBuilder(LS_CMD)
                    .redirectOutput(ProcessBuilder.Redirect.PIPE)
                    .redirectError(ProcessBuilder.Redirect.PIPE)
                    .start()

            println(proc.inputStream.bufferedReader().readText())
        } else if (args.first() == LS_CMD || args.first() == LOCATE_CMD) {
            val queue: ConcurrentLinkedQueue<Server.Response> = ConcurrentLinkedQueue()
            logger.info(tag, "args: [{}]", args.joinToString(", "))
            var queries: List<Server.Query> = servers.map { server ->
                logger.debug(tag, "searching ${server.name}...")
                server.search(args) { response ->
                    logger.debug(tag, "response: {}", response)
                    queue.add(response)
                }
            }
            while (queries.isNotEmpty()) {
                while (true) {
                    val result = queue.poll() ?: break
                    if (args.first() == LOCATE_CMD) {
                        presenter.displayMachinesOnly(result)

                    } else {
                        presenter.displayResponse(result)
                    }
                }
                queries = filterQueries(queries)
            }
        } else {
            presenter.displayHelp(helpGenerator.getHelpMessage())
            return
        }
    }

    private fun filterQueries(queries: List<Server.Query>): List<Server.Query> {
        return queries.filterNot { query ->
            query.isComplete()
        }
    }
}
