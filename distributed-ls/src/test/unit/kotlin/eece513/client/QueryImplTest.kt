import eece513.DummyLogger
import org.junit.Test

import org.junit.Assert.*
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class QueryImplTest {
    private val ip = InetAddress.getByName("127.0.0.1")
    private val port = 6969

    @Test
    fun run__send_command_line_args() {
        val ready = AtomicBoolean(false)
        val args = arrayOf("command", "line", "args")

        thread {
            while (!ready.get()) { /* block until ready is true */
            }

            QueryImpl(ip, port, "id", args, DummyLogger(), { }).run()
        }

        ServerSocket(port).use { ss ->
            ready.set(true)
            ss.accept().use { socket ->
                val isr = InputStreamReader(socket.inputStream)
                val br = BufferedReader(isr)

                assertArrayEquals(br.readLines().toTypedArray(), args)
            }
        }
    }

    @Test
    fun run__receive_single_line_result() {
        val expected = GrepClient.Server.Response.Result("id", listOf("foo bar baz"))

        thread {
            ServerSocket(port).use { ss ->
                ss.accept().use { socket ->
                    val osw = OutputStreamWriter(socket.outputStream)
                    val bw = BufferedWriter(osw)

                    bw.write("R:${expected.result.size}\n")
                    expected.result.forEach { line ->
                        bw.write("$line\n")
                    }
                    bw.flush()

                    socket.shutdownOutput()
                }
            }
        }

        QueryImpl(ip, port, "id", arrayOf(), DummyLogger()) { response ->
            assertEquals(response, expected)
        }.run()
    }

    @Test
    fun run__receive_multi_line_result() {
        val expected = GrepClient.Server.Response.Result("id", listOf("foo", "bar", "baz"))

        thread {
            ServerSocket(port).use { ss ->
                ss.accept().use { socket ->
                    val osw = OutputStreamWriter(socket.outputStream)
                    val bw = BufferedWriter(osw)

                    bw.write("R:${expected.result.size}\n")
                    expected.result.forEach { line ->
                        bw.write("$line\n")
                    }
                    bw.flush()

                    socket.shutdownOutput()
                }
            }
        }

        QueryImpl(ip, port, "id", arrayOf(), DummyLogger()) { response ->
            assertEquals(response, expected)
        }.run()
    }

    @Test
    fun run__receive_single_line_error() {
        val expected = GrepClient.Server.Response.Error("id", listOf("foo bar baz"))

        thread {
            ServerSocket(port).use { ss ->
                ss.accept().use { socket ->
                    val osw = OutputStreamWriter(socket.outputStream)
                    val bw = BufferedWriter(osw)

                    bw.write("E:${expected.result.size}\n")
                    expected.result.forEach { line ->
                        bw.write("$line\n")
                    }
                    bw.flush()

                    socket.shutdownOutput()
                }
            }
        }

        QueryImpl(ip, port, "id", arrayOf(), DummyLogger()) { response ->
            assertEquals(response, expected)
        }.run()
    }

    @Test
    fun run__receive_multi_line_error() {
        val expected = GrepClient.Server.Response.Error("id", listOf("foo", "bar", "baz"))

        thread {
            ServerSocket(port).use { ss ->
                ss.accept().use { socket ->
                    val osw = OutputStreamWriter(socket.outputStream)
                    val bw = BufferedWriter(osw)

                    bw.write("E:${expected.result.size}\n")
                    expected.result.forEach { line ->
                        bw.write("$line\n")
                    }
                    bw.flush()

                    socket.shutdownOutput()
                }
            }
        }

        QueryImpl(ip, port, "id", arrayOf(), DummyLogger()) { response ->
            assertEquals(response, expected)
        }.run()
    }

    @Test
    fun run__connection_error() {
        QueryImpl(ip, port, "id", arrayOf(), DummyLogger()) { response ->
            assertEquals(response, GrepClient.Server.Response.Error("id", listOf("Server Unreachable")))
        }.run()
    }

    @Test
    fun run__socket_timeout_exception() {
        val latch = CountDownLatch(2)

        thread {
            ServerSocket(port).use { ss ->
                ss.accept().use {
                    Thread.sleep(100)
                    latch.countDown()
                }
            }
        }

        val socket = Socket()
        socket.soTimeout = 1

        QueryImpl(ip, port, "id", arrayOf(), DummyLogger()) { response ->
            latch.countDown()
            assertEquals(response, GrepClient.Server.Response.Error("id", listOf("Server Unreachable")))
        }.run(socket)

        latch.await(1, TimeUnit.SECONDS)
    }

    @Test
    fun isComplete() {
        val expected = GrepClient.Server.Response.Result("id", listOf("foo"))

        thread {
            ServerSocket(port).use { ss ->
                ss.accept().use { socket ->
                    val osw = OutputStreamWriter(socket.outputStream)
                    val bw = BufferedWriter(osw)

                    bw.write("R:${expected.result.size}\n")
                    expected.result.forEach { line ->
                        bw.write("$line\n")
                    }
                    bw.flush()

                    socket.shutdownOutput()
                }
            }
        }

        val latch = CountDownLatch(1)
        QueryImpl(ip, port, "id", arrayOf(), DummyLogger()) {
            latch.countDown()
        }.run()

        latch.await(1, TimeUnit.SECONDS)

        if (latch.count > 0) fail()
    }
}