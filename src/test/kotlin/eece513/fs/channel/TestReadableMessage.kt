package eece513.fs.channel

import eece513.fs.message.ReadableMessageFactory
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel

class TestReadableMessage(private vararg val reads: Int) : ReadableMessageFactory.ReadableMessage {
    private var readCount = 0
    private var bytes = ArrayList<Byte>()

    override val complete: Boolean
        get() = readCount == reads.size

    override val byteArray: ByteArray?
        get() = if (complete) bytes.toByteArray() else null

    override fun read(channel: ReadableByteChannel) {
        val size = reads[readCount++]
        if (size == 0) return

        // read the indicated number of bytes
        val buffer = ByteBuffer.allocate(size)
        channel.read(buffer)

        // ..and push them onto the list of read bytes
        buffer.array().forEach { bytes.add(it) }
    }
}