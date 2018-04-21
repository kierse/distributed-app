package eece513.common.util

import org.junit.Assert.*
import org.junit.Test
import java.io.File
import java.nio.file.Files

class FileIOServerTest {
    @Test
    fun read__lines_from_file() {
        val tmpDir = Files.createTempDirectory("foo").toFile()
        val file = File(tmpDir, "servers.log")

        file.writeText("127.0.0.1")
        val servers = FileIO().ReadLinesAsInetAddress(file.absolutePath)

        assertEquals(servers.first().hostAddress, "127.0.0.1")
    }
}