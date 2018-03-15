import com.nhaarman.mockito_kotlin.mock
import eece513.client.FileIO
import eece513.client.GrepClient
import eece513.client.ServerImpl
import eece513.helper.ResultCounterPresenter
import eece513.helper.pathToAssets
import org.junit.Assert.assertEquals
import org.junit.Test

/**
 * The following tests should reflect common data outlined in:
 * src/main/kotlin/eece513/assets/logFiles/notes.txt
 */
class CommonToAllServersTest {
    private val fileIo = FileIO()

    @Test
    fun verify_common_data_1() {
        val presenter = ResultCounterPresenter()
        val logger = TinyLogWrapper()

        var count = 1
        val servers = fileIo.ReadLinesAsInetAddress(pathToAssets()).map { address ->
            ServerImpl(address, SERVER_PORT, (count++).toString(), logger)
        }

        GrepClient(presenter, mock(), logger, servers)
                .search(arrayOf("Lorem ipsum"))

        assertEquals(6, presenter.count)
    }

    @Test
    fun verify_common_data_2() {
        val presenter = ResultCounterPresenter()
        val logger = DummyLogger()

        var count = 1
        val servers = fileIo.ReadLinesAsInetAddress(pathToAssets()).map { address ->
            ServerImpl(address, SERVER_PORT, (count++).toString(), logger)
        }

        GrepClient(presenter, mock(), logger, servers)
                .search(arrayOf("broadway.sfn.saskatoon.sk.ca"))

        assertEquals(6, presenter.count)
    }

    @Test
    fun verify_common_data_with_multiple_results_per_server() {
        val presenter = ResultCounterPresenter()
        val logger = DummyLogger()

        var count = 1
        val servers = fileIo.ReadLinesAsInetAddress(pathToAssets()).map { address ->
            ServerImpl(address, SERVER_PORT, (count++).toString(), logger)
        }

        GrepClient(presenter, mock(), logger, servers)
                .search(arrayOf("httpd"))

        assertEquals(33, presenter.count)
    }
}