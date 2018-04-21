package eece513.fs.ring

import eece513.common.FILE_SYSTEM_SEPARATOR
import eece513.common.PROMPT_WINDOW
import eece513.fs.DummyLogger
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.withTimeoutOrNull
import org.junit.Test

import org.junit.Assert.*
import org.junit.Before
import java.io.File
import java.net.InetAddress
import java.nio.file.Files
import java.time.Instant
import java.util.concurrent.TimeUnit

class FileSystemTest {
    private lateinit var root: File
    private lateinit var files: File
    private lateinit var servers: File
    private lateinit var seedData: MutableMap<String, MutableList<String>>
    private lateinit var fs: FileSystem

    @Before
    fun setup() {
        root = Files.createTempDirectory("foo").toFile()
        files = File(root, "files")
        servers = File(root, "servers")
        seedData = mutableMapOf()
        fs = FileSystem(files.absolutePath, servers.absolutePath, DummyLogger(), seedData)
    }

    @Test
    fun initialize__write_servers_to_disk() {
        val address = InetAddress.getByName("127.0.0.1")

        fs.initialize(address.hostName)

        runBlocking {
            withTimeoutOrNull(1L, TimeUnit.SECONDS) {
                while (!servers.exists()) { }
            }

            assertTrue(servers.exists())
        }
    }

    @Test
    fun initialize__make_files_dir() {
        fs.initialize("127.0.0.1")

        runBlocking {
            val exists = withTimeoutOrNull(1L, TimeUnit.SECONDS) {
                while (!files.exists()) { }
                true
            } ?: false

            assertTrue(exists)
        }
    }

    @Test
    fun initialize__remove_stale_files_on_startup() {
        files.mkdirs()
        File(files, "foo").createNewFile()
        File(files, "bar").createNewFile()
        File(files, "baz").createNewFile()

        assertFalse(files.listFiles().isEmpty())

        fs.initialize("127.0.0.1")

        runBlocking {
            val exists = withTimeoutOrNull(1L, TimeUnit.SECONDS) {
                while (!files.listFiles().isEmpty()) { }
                true
            } ?: false

            assertTrue(exists)
        }
    }

    @Test
    fun removeFile__all_versions_of_file() {
        files.mkdirs()

        val one = File(files, "foo-1")
        one.createNewFile()

        val two = File(files, "foo-2")
        two.createNewFile()

        val three = File(files, "foo-3")
        three.createNewFile()

        assertTrue(one.exists())
        assertTrue(two.exists())
        assertTrue(three.exists())

        fs.removeFile("foo")

        runBlocking {
            withTimeoutOrNull(1L, TimeUnit.SECONDS) {
                while (one.exists() || two.exists() || three.exists()) { }
            }

            assertFalse(one.exists())
            assertFalse(two.exists())
            assertFalse(three.exists())
        }
    }

    @Test
    fun removeFile__filter() {
        files.mkdirs()

        val foo = File(files, "foo")
        foo.createNewFile()

        val other = File(files, "bar")
        other.createNewFile()

        assertTrue(foo.exists())
        assertTrue(other.exists())

        fs.removeFile("foo")

        runBlocking {
            withTimeoutOrNull(1L, TimeUnit.SECONDS) {
                while (foo.exists()) { }
            }

            assertTrue(other.exists())
        }
    }

    @Test
    fun clientShouldPromptUser() {
        assertFalse(fs.clientShouldPromptUser("foo", Instant.now()))
    }

    @Test
    fun clientShouldPromptUser__within_a_minute() {
        val fileName = fs.buildEncodedFileName("foo", Instant.now().minusSeconds(PROMPT_WINDOW - 1))
        seedData["foo"] = mutableListOf(fileName.absolutePath)

        assertTrue(fs.clientShouldPromptUser("foo", Instant.now()))
    }

    @Test
    fun clientShouldPromptUser__longer_than_a_minute() {
        val fileName = fs.buildEncodedFileName("foo", Instant.now().minusSeconds(PROMPT_WINDOW + 1))
        seedData["foo"] = mutableListOf(fileName.absolutePath)

        assertFalse(fs.clientShouldPromptUser("foo", Instant.now()))
    }

    @Test
    fun registerPutRequest() {
        val now = Instant.now()

        fs.registerPutRequest("foo", now)

        assertTrue(seedData.contains("foo"))
        assertEquals(seedData.getOrDefault("foo", emptyList<String>()).size, 1)
    }

    @Test
    fun buildEncodedFileName() {
        val now = Instant.now()

        val result = fs.buildEncodedFileName("foo", now)

        assertTrue(result.startsWith(root.absoluteFile))
        assertTrue(result.endsWith("foo$FILE_SYSTEM_SEPARATOR${now.toEpochMilli()}"))
    }

    @Test
    fun saveAddressListToDisk() {
        val address = InetAddress.getByName("127.0.0.1")

        fs.saveAddressListToDisk(listOf(address.hostName))

        runBlocking {
            withTimeoutOrNull(1L, TimeUnit.SECONDS) {
                while (!servers.exists()) { }
            }

            assertTrue(servers.exists())
        }
    }
}