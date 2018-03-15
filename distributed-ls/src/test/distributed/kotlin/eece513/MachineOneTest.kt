import com.nhaarman.mockito_kotlin.mock
import eece513.client.FileIO
import eece513.client.GrepClient
import eece513.client.ServerImpl
import eece513.helper.ResultCounterPresenter
import eece513.helper.pathToAssets
import org.junit.Assert.assertEquals
import org.junit.Test

class MachineOneTest {
    private val fileIo = FileIO()

    @Test
    fun verify_data_unique_to_server() {
        val presenter = ResultCounterPresenter()
        val logger = TinyLogWrapper()

        var count = 1
        val servers = fileIo.ReadLinesAsInetAddress(pathToAssets()).map { address ->
            ServerImpl(address, SERVER_PORT, (count++).toString(), logger)
        }

        GrepClient(presenter, mock(), logger, servers)
                .search(arrayOf("GET /wow/mexico/"))

        assertEquals(1, presenter.count)
    }
}