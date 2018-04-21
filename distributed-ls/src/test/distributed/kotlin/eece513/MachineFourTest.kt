import com.nhaarman.mockito_kotlin.mock
import eece513.SERVER_PORT
import eece513.client.Client
import eece513.client.ServerImpl
import eece513.common.TinyLogWrapper
import eece513.common.util.FileIO
import org.junit.Assert.assertEquals
import org.junit.Test

class MachineFourTest {
    private val fileIo = FileIO()

    @Test
    fun verify_data_unique_to_server_case_insensitive() {
        val presenter = ResultCounterPresenter()
        val logger = TinyLogWrapper()

        var count = 1
        val servers = fileIo.ReadLinesAsInetAddress(pathToAssets()).map { address ->
            ServerImpl(address, SERVER_PORT, (count++).toString(), logger)
        }

        Client(presenter, mock(), logger, servers)
                .execute(arrayOf("-i", "-e", "scooby", "-e", "dooby", "-e", "doo"))

        assertEquals(3, presenter.count)
    }

    @Test
    fun verify_data_unique_to_server_case_sensitive() {
        val presenter = ResultCounterPresenter()
        val logger = TinyLogWrapper()

        var count = 1
        val servers = fileIo.ReadLinesAsInetAddress(pathToAssets()).map { address ->
            ServerImpl(address, SERVER_PORT, (count++).toString(), logger)
        }

        Client(presenter, mock(), logger, servers)
                .execute(arrayOf("-e", "sCoOby", "-e", "doobY", "-e", "DOO"))

        assertEquals(3, presenter.count)
    }
}