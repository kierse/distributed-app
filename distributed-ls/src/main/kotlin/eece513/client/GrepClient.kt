package eece513.client

import eece513.*
import eece513.common.FILE_SYSTEM_SEPARATOR
import eece513.common.Logger
import eece513.common.SERVERS_FILE_PATH
import eece513.common.TinyLogWrapper
import eece513.common.util.FileIO
import eece513.common.util.getLocalFiles
import eece513.common.util.unescapeFileName
import java.util.concurrent.ConcurrentLinkedQueue


class GrepClient(
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

        fun search(args: Array<String>, onResult: (Response) -> Unit): Query
    }

    interface HelpGenerator {
        fun getHelpMessage(): String
    }

    companion object {
        private val tag: String = GrepClient::class.java.simpleName

        @JvmStatic
        fun main(args: Array<String>) {
            val logger = TinyLogWrapper(CLIENT_LOG_LOCATION)
            val presenter = PrintStreamPresenter(System.out, System.err, logger)
            val helpGenerator = GrepHelpGenerator(GREP_CMD, logger)

            val servers = FileIO().ReadLinesAsInetAddress(SERVERS_FILE_PATH).map { address ->
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
        logger.info(tag, "args: [{}]", args.joinToString(", "))

        when (args.first()) {
            LSHERE_CMD -> getLocalFiles()
                    .forEach { file ->
                        println(unescapeFileName(file.name.substringBeforeLast(FILE_SYSTEM_SEPARATOR)))
                    }

            LOCATE_CMD -> processQueries(servers, LOCATE_CMD, args[1]) { response ->
                if (response.result.first() == "1") GrepClient.Server.Response.Result(response.name, listOf(response.name))
                else null
            }

            LS_CMD -> processQueries(servers.subList(0, 1), LS_CMD)

            else -> presenter.displayHelp(helpGenerator.getHelpMessage())
        }
    }

    private fun processQueries(
            servers: List<Server>, cmd: String, arg: String = "", onResult: ((Server.Response) -> Server.Response?)? = null
    ) {
        val queue: ConcurrentLinkedQueue<Server.Response> = ConcurrentLinkedQueue()

        var queries = servers.map { server ->
            logger.debug(tag, "searching ${server.name}...")
            server.search(arrayOf(cmd, arg)) { response ->
                logger.debug(tag, "response: {}", response)

                val result = if (onResult == null) response else onResult(response)
                if (result != null)  queue.add(result)
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
