import eece513.DummyLogger
import org.junit.Test

import org.junit.Assert.*

class GrepQueryServiceTest {
    @Test
    fun search__result() {
        GrepQueryService("echo", "file", DummyLogger())
                .search(
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
                .search(
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