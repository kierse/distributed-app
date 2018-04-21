import com.nhaarman.mockito_kotlin.*
import eece513.DummyLogger
import eece513.GREP_CMD
import eece513.server.GrepQueryService
import eece513.server.LocateQueryService
import eece513.server.LsQueryService
import eece513.server.Server
import org.junit.Test

class ServerTest {
    private class TestConnectionListener(
            private val connection: Server.ConnectionListener.Connection
    ) : Server.ConnectionListener {
        override fun listen(onQuery: (Server.ConnectionListener.Connection) -> Unit) {
            onQuery(connection)
        }
    }

    class TestQueryService private constructor(
            private val result: String?, private val error: Array<String>?
    ) : Server.QueryService {
        companion object {
            fun fromResult(result: String) = TestQueryService(result, null)
            fun fromError(error: Array<String>) = TestQueryService(null, error)
        }

        override fun execute(args: Array<String>, onResult: (String) -> Unit, onError: (Array<String>) -> Unit) {
            when {
                result != null -> onResult(result)
                error != null -> onError(error)
                else -> throw IllegalStateException()
            }
        }
    }

    @Test
    fun run__listen_for_connection() {
        val queryService = mock<GrepQueryService>()
        val lsService = mock<LsQueryService>()
        val locateService = mock<LocateQueryService>()

        val connectionListener = mock<Server.ConnectionListener>()
        doNothing().whenever(connectionListener).listen(any())

        Server(queryService, lsService, locateService, connectionListener, DummyLogger())
                .run()

        verify(connectionListener).listen(any())
    }

    @Test
    fun run__get_query_args_on_connection() {
        val lsService = mock<LsQueryService>()
        val locateService = mock<LocateQueryService>()

        val queryService = mock<GrepQueryService>()
        doNothing().whenever(queryService).execute(any(), any(), any())

        val connection = mock<Server.ConnectionListener.Connection>()
        whenever(connection.getQueryArgs()).thenReturn(arrayOf(GREP_CMD, "args"))

        val listener = TestConnectionListener(connection)

        Server(queryService, lsService, locateService, listener, DummyLogger())
                .run()

        verify(connection).getQueryArgs()
    }

    @Test
    fun run__query_on_connection() {
        val args = arrayOf(GREP_CMD, "b", "c")

        val queryService = mock<GrepQueryService>()
        val lsService = mock<LsQueryService>()
        val locateService = mock<LocateQueryService>()

        val connection = mock<Server.ConnectionListener.Connection>()
        whenever(connection.getQueryArgs()).thenReturn(args)

        val listener = TestConnectionListener(connection)

        Server(queryService, lsService, locateService, listener, DummyLogger())
                .run()

        verify(queryService).execute(eq(args.copyOfRange(1, args.size)), any(), any())
    }

    @Test
    fun run__send_query_result_to_connection() {
        val queryService = TestQueryService.fromResult("foo")
        val lsService = mock<LsQueryService>()
        val locateService = mock<LocateQueryService>()

        val connection = mock<Server.ConnectionListener.Connection>()
        whenever(connection.getQueryArgs()).thenReturn(arrayOf(GREP_CMD, "args"))

        val listener = TestConnectionListener(connection)

        Server(queryService, lsService, locateService, listener, DummyLogger())
                .run()

        verify(connection).sendResult("foo")
    }

    @Test
    fun run__send_query_error_to_connection() {
        val error = arrayOf("some", "error")
        val queryService = TestQueryService.fromError(error)
        val lsService = mock<LsQueryService>()
        val locateService = mock<LocateQueryService>()

        val connection = mock<Server.ConnectionListener.Connection>()
        whenever(connection.getQueryArgs()).thenReturn(arrayOf(GREP_CMD, "args"))

        val listener = TestConnectionListener(connection)

        Server(queryService, lsService, locateService, listener, DummyLogger())
                .run()

        verify(connection).sendError(error)
    }
}