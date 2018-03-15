import com.nhaarman.mockito_kotlin.*
import eece513.DummyLogger
import org.junit.Test

class GrepClientTest {
    @Test
    fun search__display_help_message_when_no_command_line_args() {
        val servers = listOf<GrepClient.Server>(mock())
        val presenter = mock<GrepClient.Presenter>()
        val generator = mock<GrepClient.HelpGenerator>()

        whenever(generator.getHelpMessage()).thenReturn("error!")

        GrepClient(presenter, generator, DummyLogger(), servers).search(arrayOf())

        verify(presenter).displayHelp("error!")
    }

    @Test
    fun search__call_search_on_all_servers() {
        val query = mock<GrepClient.Server.Query>()
        whenever(query.isComplete()).thenReturn(true)

        val server = mock<GrepClient.Server>()
        whenever(server.search(any(), any())).thenReturn(query)

        GrepClient(mock(), mock(), DummyLogger(), listOf(server, server))
                .search(arrayOf("args"))

        verify(server, times(2)).search(eq(arrayOf("args")), any())
    }

    @Test
    fun search__display_response() {
        class TestServer(
                private val query: GrepClient.Server.Query,
                private val response: GrepClient.Server.Response
        ) : GrepClient.Server {
            override val name: String = "test"

            override fun search(
                    args: Array<String>, onResult: (GrepClient.Server.Response) -> Unit
            ): GrepClient.Server.Query {
                onResult.invoke(response)
                return query
            }
        }

        val presenter = mock<GrepClient.Presenter>()

        val query = mock<GrepClient.Server.Query>()
        whenever(query.isComplete()).thenReturn(true)

        val response = mock<GrepClient.Server.Response>()
        val server = TestServer(query, response)

        GrepClient(presenter, mock(), DummyLogger(), listOf(server))
                .search(arrayOf("args"))

        verify(presenter).displayResponse(response)
    }
}