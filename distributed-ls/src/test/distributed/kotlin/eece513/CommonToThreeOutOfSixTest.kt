import com.nhaarman.mockito_kotlin.mock
import eece513.client.FileIO
import eece513.client.GrepClient
import eece513.client.ServerImpl
import eece513.helper.ResultCounterPresenter
import eece513.helper.pathToAssets
import org.junit.Assert.assertEquals
import org.junit.Test

class CommonToThreeOutOfSixTest {
    private val fileIo = FileIO()

    @Test
    fun verify_data_common_to_servers_2_through_4() {
        val presenter = ResultCounterPresenter()
        val logger = TinyLogWrapper()

        var count = 1
        val servers = fileIo.ReadLinesAsInetAddress(pathToAssets()).map { address ->
            ServerImpl(address, SERVER_PORT, (count++).toString(), logger)
        }

        GrepClient(presenter, mock(), logger, servers)
                .search(arrayOf("akira.link.ca"))

        assertEquals(3, presenter.count)
    }
}