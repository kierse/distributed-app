import com.nhaarman.mockito_kotlin.*
import eece513.client.FileIO
import org.junit.Assert.*
import org.junit.Test

class FileIOServerTest {
    @Test
    fun get__server_file() {
        var loc = FileIO().GetGrepDataLoc()
        loc = loc.split("\\").last()
        val extensionLoc = loc.split(".").last()

        assertEquals(extensionLoc, "log")
        assertTrue(loc.contains("machine"))
    }
}