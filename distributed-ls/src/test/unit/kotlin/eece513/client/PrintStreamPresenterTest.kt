package eece513.client

import eece513.DummyLogger
import org.junit.Test

import org.junit.Assert.*
import java.io.ByteArrayOutputStream
import java.io.PrintStream

class PrintStreamPresenterTest {
    @Test
    fun displayResponse__result() {
        val result = Client.Server.Response.Result("name", listOf("result"))
        val stdOutStream = ByteArrayOutputStream()
        val stdErrStream = ByteArrayOutputStream()

        PrintStreamPresenter(PrintStream(stdOutStream), PrintStream(stdErrStream), DummyLogger())
                .displayResponse(result)

        // Note: trailing \n is because presenter calls println()
        assertEquals(stdOutStream.toString(), "${result.name}:${result.result.first()}\n")
        assertTrue(stdErrStream.toString().isEmpty())
    }

    @Test
    fun displayResponse__error() {
        val result = Client.Server.Response.Error("name", listOf("error!"))
        val stdOutStream = ByteArrayOutputStream()
        val stdErrStream = ByteArrayOutputStream()

        PrintStreamPresenter(PrintStream(stdOutStream), PrintStream(stdErrStream), DummyLogger())
                .displayResponse(result)

        // Note: trailing \n is because presenter calls println()
        assertEquals(stdErrStream.toString(), "${result.name}:${result.result.first()}\n")
        assertTrue(stdOutStream.toString().isEmpty())
    }

    @Test
    fun displayHelp() {
        val stdOutStream = ByteArrayOutputStream()
        val stdErrStream = ByteArrayOutputStream()

        PrintStreamPresenter(PrintStream(stdOutStream), PrintStream(stdErrStream), DummyLogger())
                .displayHelp("hello world!")

        assertEquals(stdErrStream.toString(), "hello world!\n")
        assertTrue(stdOutStream.toString().isEmpty())
    }
}