package eece513.server

import eece513.DummyLogger
import org.junit.Assert.*
import org.junit.Test
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class ConnectionImplTest {
    private val ip = InetAddress.getByName("127.0.0.1")
    private val port = 6969

    @Test
    fun getQueryArgs() {
        val args = arrayOf("command", "line", "args")
        val ready = AtomicBoolean(false)

        thread {
            ServerSocket(port).use { ss ->
                ready.set(true)

                ss.accept().use { socket ->
                    val osw = OutputStreamWriter(socket.outputStream)
                    val bw = BufferedWriter(osw)

                    args.forEach { arg ->
                        bw.write("$arg\n")
                    }
                    bw.flush()

                    socket.shutdownOutput()
                }
            }
        }

        while (!ready.get()) { /* wait till ready */
        }

        Socket(ip, port).use { socket ->
            assertArrayEquals(ConnectionImpl(socket, DummyLogger()).getQueryArgs(), args)
        }
    }

    @Test
    fun sendResults() {
        val ready = AtomicBoolean(false)

        thread {
            while (!ready.get()) { /* wait till ready */
            }

            Socket(ip, port).use { socket ->
                ConnectionImpl(socket, DummyLogger()).sendResult("foo")
            }
        }

        ServerSocket(port).use { ss ->
            ready.set(true)

            ss.accept().use { socket ->
                val isw = InputStreamReader(socket.inputStream)
                val br = BufferedReader(isw)

                assertEquals("R:1", br.readLine())
                assertEquals("foo", br.readLine())
                assertNull(br.readLine())
            }
        }
    }

    @Test
    fun sendError() {
        val ready = AtomicBoolean(false)

        thread {
            while (!ready.get()) { /* wait till ready */
            }

            Socket(ip, port).use { socket ->
                ConnectionImpl(socket, DummyLogger()).sendError(arrayOf("foo", "bar", "baz"))
            }
        }

        ServerSocket(port).use { ss ->
            ready.set(true)

            ss.accept().use { socket ->
                val isw = InputStreamReader(socket.inputStream)
                val br = BufferedReader(isw)

                assertEquals("E:3", br.readLine())
                assertEquals("foo", br.readLine())
                assertEquals("bar", br.readLine())
                assertEquals("baz", br.readLine())
                assertNull(br.readLine())
            }
        }
    }
}