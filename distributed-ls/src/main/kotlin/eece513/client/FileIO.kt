package eece513.client

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

    /*
    * Gets the absolute path of the log files with the name pattern machine.*.log
    * */
    fun GetGrepDataLoc(): String {
        val regex = Regex("""machine\.\d\.log$""")

        // Tries to find a matched file name
        File(System.getProperty("user.dir")).walkTopDown().forEach { file ->
            if (file.name.matches(regex)) {
                return file.absolutePath
            }
        }

        // Otherwise return error message
        return "machine.*.log file not found"
    }
}