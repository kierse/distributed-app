package eece513.fs.mapper

interface ObjectMapper<T> {
    fun toByteArray(obj: T): ByteArray
    fun toObject(byteArray: ByteArray): T

    class EmptyByteArrayException(msg: String) : Throwable(msg)
    class ParseException private constructor(msg: String?, e: Exception?) : Throwable(msg, e) {
        constructor(e: Exception) : this(null, e)
    }
}