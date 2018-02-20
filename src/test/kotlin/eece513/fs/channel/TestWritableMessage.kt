package eece513.fs.channel

import eece513.fs.message.SendableMessageFactory
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel

class TestWritableMessage(
        byteArray: ByteArray,
        private vararg val writes: Int
) : SendableMessageFactory.SendableMessage {
    private var writeCount = 0
    private val buffer = ByteBuffer.allocate(byteArray.size)

    init {
        buffer.put(byteArray)
        buffer.flip()
    }

    override val complete: Boolean
        get() = writeCount == writes.size

    override fun send(channel: WritableByteChannel) {
        val size = writes[writeCount++]
        if (size == 0) return

        val buf = ByteBuffer.allocate(size)
        0.until(size).forEach {
            buf.put(buffer.get())
        }
        buf.flip()

        channel.write(buf)
    }
}