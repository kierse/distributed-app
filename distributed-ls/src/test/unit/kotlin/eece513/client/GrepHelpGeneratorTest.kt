package eece513.client

import eece513.DummyLogger
import eece513.GREP_CMD
import org.junit.Test

import org.junit.Assert.*

class GrepHelpGeneratorTest {

    @Test
    fun getHelpMessage() {
        val result = GrepHelpGenerator(GREP_CMD, DummyLogger()).getHelpMessage()

        assertNotNull(result)
        assertTrue(result.contains("usage: grep"))
    }
}