package eece513.server

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import eece513.DummyLogger
import org.junit.Test

import java.net.InetAddress
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class SocketConnectionListenerTest {
    private val ip = InetAddress.getByName("127.0.0.1")

    @Test
    fun listen__invoke_callback_on_socket_connection() {
        val onQuery = mock<(Server.ConnectionListener.Connection) -> Unit>()
        val loop = AtomicBoolean(true)

        val thread = thread {
            SocketConnectionListener(6968, DummyLogger(), loop)
                    .listen(onQuery)
        }

        // give the listener time to start
        Thread.sleep(500)

        Socket(ip, 6968).use { /* do nothing */ }

        loop.set(false)
        thread.join()

        verify(onQuery).invoke(any())
    }

    @Test
    fun listen__listens_for_new_connect_when_old_closes() {
        val onQuery = mock<(Server.ConnectionListener.Connection) -> Unit>()
        val loop = AtomicBoolean(true)

        val thread = thread {
            SocketConnectionListener(6969, DummyLogger(), loop)
                    .listen(onQuery)
        }

        // give the listener time to start
        Thread.sleep(500)

        // first connection
        Socket(ip, 6969).use { /* do nothing */ }

        // give the listener time to cleanup before accepting new connections
        Thread.sleep(500)

        // second connection
        Socket(ip, 6969).use { /* do nothing */ }

        loop.set(false)
        thread.join()

        verify(onQuery, times(2)).invoke(any())
    }
}