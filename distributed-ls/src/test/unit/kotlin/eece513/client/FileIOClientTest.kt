import org.junit.Test
import org.junit.Assert.*
import java.io.File

class FileIOClientTest {
    private val path = arrayOf(
            System.getProperty("user.dir"),
            "src",
            "test",
            "unit",
            "kotlin",
            "eece513",
            "DummyServer.txt"
    ).joinToString(File.separator)

    @Test
    fun read__lines_from_file() {
        val servers = FileIO().ReadLinesAsInetAddress(path)

        assertEquals(servers.first().hostAddress, "127.0.0.1")
    }
}