import eece513.DummyLogger
import eece513.server.GrepQueryService
import org.junit.Test

import org.junit.Assert.*

class GrepQueryServiceTest {
    @Test
    fun search__result() {
        GrepQueryService("echo", "file", DummyLogger())
                .execute(
                        arrayOf("a", "b", "c"),
                        { result ->
                            assertEquals(result, "a b c file")
                        },
                        { fail() }
                )
    }

    @Test
    fun search__error() {
        GrepQueryService("grep", "--help", DummyLogger())
                .execute(
                        arrayOf("a", "b", "c"),
                        { fail() },
                        { lines ->
                            val error = lines.joinToString("\n")
                            assertTrue(
                                    error.startsWith("usage: grep")
                            )
                        }
                )
    }
}