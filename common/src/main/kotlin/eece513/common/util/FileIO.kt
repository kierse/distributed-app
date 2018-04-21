package eece513.common.util

import java.io.*
import java.net.InetAddress

class FileIO {
    /*
    * Reads a file with IPs and returns an array of InetAddress
    * */
    fun ReadLinesAsInetAddress(path: String): List<InetAddress> {
        val inputStream: InputStream = File(path).inputStream()

        return inputStream
                .bufferedReader()
                .readLines()
                .filter { it.isNotEmpty() }
                .map {
                    InetAddress.getByName(it)
                }
    }
}