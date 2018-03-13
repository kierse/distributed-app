package eece513.common.mapper

interface ObjectMapper<out T> {
    fun toObjectOrNull(byteArray: ByteArray): T?

    fun toObject(byteArray: ByteArray): T {
        return toObjectOrNull(byteArray)
                ?: throw ParseException("Unable to parse ByteArray and construct object!")
    }
}

interface ByteMapper<in T> {
    fun toByteArray(obj: T): ByteArray
}

class EmptyByteArrayException(msg: String) : Throwable(msg)
class ParseException(msg: String) : Throwable(msg)
