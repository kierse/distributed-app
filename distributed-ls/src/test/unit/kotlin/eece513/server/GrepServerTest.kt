import com.nhaarman.mockito_kotlin.*
import eece513.DummyLogger
import org.junit.Test

import org.junit.Assert.*

class GrepServerTest {
    private class TestConnectionListener(
            private val connection: GrepServer.ConnectionListener.Connection
    ) : GrepServer.ConnectionListener {
        override fun listen(onQuery: (GrepServer.ConnectionListener.Connection) -> Unit) {
            onQuery(connection)
        }
    }

    class TestQueryService private constructor(
            private val result: String?, private val error: Array<String>?
    ) : GrepServer.QueryService {
        companion object {
            fun fromResult(result: String) = TestQueryService(result, null)
            fun fromError(error: Array<String>) = TestQueryService(null, error)
        }

        override fun search(args: Array<String>, onResult: (String) -> Unit, onError: (Array<String>) -> Unit) {
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

        val connectionListener = mock<GrepServer.ConnectionListener>()
        doNothing().whenever(connectionListener).listen(any())

        GrepServer(queryService, connectionListener, DummyLogger())
                .run()

        verify(connectionListener).listen(any())
    }

    @Test
    fun run__get_query_args_on_connection() {
        val queryService = mock<GrepQueryService>()
        doNothing().whenever(queryService).search(any(), any(), any())

        val connection = mock<GrepServer.ConnectionListener.Connection>()
        whenever(connection.getQueryArgs()).thenReturn(arrayOf())

        val listener = TestConnectionListener(connection)

        GrepServer(queryService, listener, DummyLogger())
                .run()

        verify(connection).getQueryArgs()
    }

    @Test
    fun run__query_on_connection() {
        val args = arrayOf("a", "b", "c")

        val queryService = mock<GrepQueryService>()

        val connection = mock<GrepServer.ConnectionListener.Connection>()
        whenever(connection.getQueryArgs()).thenReturn(args)

        val listener = TestConnectionListener(connection)

        GrepServer(queryService, listener, DummyLogger())
                .run()

        verify(queryService).search(eq(args), any(), any())
    }

    @Test
    fun run__send_query_result_to_connection() {
        val queryService = TestQueryService.fromResult("foo")

        val connection = mock<GrepServer.ConnectionListener.Connection>()
        whenever(connection.getQueryArgs()).thenReturn(arrayOf())

        val listener = TestConnectionListener(connection)

        GrepServer(queryService, listener, DummyLogger())
                .run()

        verify(connection).sendResult("foo")
    }

    @Test
    fun run__send_query_error_to_connection() {
        val error = arrayOf("some", "error")
        val queryService = TestQueryService.fromError(error)

        val connection = mock<GrepServer.ConnectionListener.Connection>()
        whenever(connection.getQueryArgs()).thenReturn(arrayOf())

        val listener = TestConnectionListener(connection)

        GrepServer(queryService, listener, DummyLogger())
                .run()

        verify(connection).sendError(error)
    }
}