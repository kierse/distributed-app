import com.nhaarman.mockito_kotlin.*
import eece513.DummyLogger
import eece513.GREP_CMD
import eece513.client.Client
import org.junit.Test

class ClientTest {
    @Test
    fun search__display_help_message_when_no_command_line_args() {
        val servers = listOf<Client.Server>(mock())
        val presenter = mock<Client.Presenter>()
        val generator = mock<Client.HelpGenerator>()

        whenever(generator.getHelpMessage()).thenReturn("error!")

        Client(presenter, generator, DummyLogger(), servers).execute(arrayOf())

        verify(presenter).displayHelp("error!")
    }

    @Test
    fun search__call_search_on_all_servers() {
        val query = mock<Client.Server.Query>()
        whenever(query.isComplete()).thenReturn(true)

        val server = mock<Client.Server>()
        whenever(server.execute(any(), any())).thenReturn(query)

        Client(mock(), mock(), DummyLogger(), listOf(server, server))
                .execute(arrayOf(GREP_CMD))

        verify(server, times(2)).execute(eq(arrayOf(GREP_CMD)), any())
    }

    @Test
    fun search__display_response() {
        class TestServer(
                private val query: Client.Server.Query,
                private val response: Client.Server.Response
        ) : Client.Server {
            override val name: String = "test"

            override fun execute(
                    args: Array<String>, onResult: (Client.Server.Response) -> Unit
            ): Client.Server.Query {
                onResult.invoke(response)
                return query
            }
        }

        val presenter = mock<Client.Presenter>()

        val query = mock<Client.Server.Query>()
        whenever(query.isComplete()).thenReturn(true)

        val response = mock<Client.Server.Response>()
        val server = TestServer(query, response)

        Client(presenter, mock(), DummyLogger(), listOf(server))
                .execute(arrayOf(GREP_CMD))

        verify(presenter).displayResponse(response)
    }
}