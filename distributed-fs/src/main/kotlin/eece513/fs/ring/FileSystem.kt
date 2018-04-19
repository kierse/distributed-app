package eece513.fs.ring

import eece513.common.*
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.launch
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.net.InetSocketAddress
import java.time.Instant

class FileSystem(private val logger: Logger) {
    private val tag = FileSystem::class.java.simpleName

    private val remoteFileToTimestamp = mutableMapOf<String, MutableList<String>>()

    fun initialize() {
        // make sure filesystem directory exists
        File(FILE_SYSTEM_PATH).mkdirs()

        // make sure filesystem is empty
        // NOTE: this will also delete the servers.txt file!!
// TODO
//        File(FILE_SYSTEM_PATH)
//                .listFiles()
//                .forEach { file ->
//                    file.delete()
//                }
// TODO
    }

    fun removeFile(remoteName: String) {
        launch(CommonPool) {
            File(FILE_SYSTEM_PATH)
                    .listFiles()
                    .filter { file ->
                        file.name.startsWith(escapeRemoteFileName(remoteName))
                    }
                    .forEach { file ->
                        // Note: assuming we are the file owner here
                        logger.debug(tag, "removing file: $FILE_SYSTEM_PATH/${file.name}")
                        file.delete()
                    }
        }
    }

    fun getLatestVersion(remoteName: String): File? {
        return File(FILE_SYSTEM_PATH)
                .listFiles()
                .filter { file ->
                    file.name.startsWith(escapeRemoteFileName(remoteName))
                }
                .sortedBy { it.name }
                .lastOrNull()

    }

    fun clientShouldPromptUser(remoteName: String, timestamp: Instant): Boolean {
        val filenames = remoteFileToTimestamp.getOrDefault(remoteName, emptyList<String>())
        for (filename in filenames.reversed()) {
            val (_, oldTimestamp) = parseFileName(filename)

            if (oldTimestamp.plusSeconds(PROMPT_WINDOW) > timestamp) return true
        }

        return false
    }

    fun registerPutRequest(remoteFileName: String, timestamp: Instant) {
        val pathToFile = buildEncodedFileName(remoteFileName, timestamp)

        val existing = remoteFileToTimestamp[remoteFileName] ?: mutableListOf()
        existing.add(pathToFile.absolutePath)
        remoteFileToTimestamp[remoteFileName] = existing
    }

    private fun parseFileName(pathToFile: String): Pair<String, Instant> {
        val name = pathToFile.substringBeforeLast("-")
        val number = pathToFile.substringAfterLast("-")

        return Pair(
                unescapeRemoteFileName(name),
                Instant.ofEpochMilli(number.toLong())
        )
    }

    fun buildEncodedFileName(remoteFileName: String, timestamp: Instant): File {
        return File(FILE_SYSTEM_PATH + "/${escapeRemoteFileName(remoteFileName)}-${timestamp.toEpochMilli()}")
    }

    private fun decodeHumanReadableFileName(pathToFile: String): String {
        val encodedFileName = pathToFile.substringAfterLast("/")
        return unescapeRemoteFileName(encodedFileName.substringBeforeLast("-"))
    }

    private fun escapeRemoteFileName(remoteName: String): String {
        return remoteName.replace("/", "+")
    }

    private fun unescapeRemoteFileName(escapedRemoteName: String): String {
        return escapedRemoteName.replace("+", "/")
    }

    fun confirmPut(@Suppress("UNUSED_PARAMETER") pathToFile: String) {
        val lastRelevantTime = Instant.now().minusSeconds(PROMPT_WINDOW)

        for (filenames in remoteFileToTimestamp.values) {
            val iterator = filenames.iterator()
            while (iterator.hasNext()) {
                val filename = iterator.next()
                val (_, timestamp) = parseFileName(filename)

                if (lastRelevantTime > timestamp) iterator.remove()
            }
        }
    }

    fun syncFiles(source: InetSocketAddress) {
        launch(CommonPool) {
            val remoteAddress = source.hostName
            val lsCmd = arrayOf("ssh", "-i", PRIVATE_KEY_PATH, "$SSH_USER@$remoteAddress", "ls", "-1r", FILE_SYSTEM_PATH)

            val lsProc = ProcessBuilder(*lsCmd).start()

            lsProc.waitFor()

            if (lsProc.exitValue() > 0) {
                BufferedReader(InputStreamReader(lsProc.errorStream)).use { reader ->
                    logger.warn(tag, "Errors while fetching file list: ${reader.readLines().joinToString("\n")}")
                }

                return@launch
            }

            val tmpFile = createTempFile()
            BufferedReader(InputStreamReader(lsProc.inputStream)).use { reader ->
                val names = mutableSetOf<String>()
                val uniqueFiles = reader.readLines()
                        .filter { fileName ->
                            names.add(decodeHumanReadableFileName(fileName))
                        }

                if (uniqueFiles.isEmpty()) {
                    logger.info(tag, "No files to sync!")
                    return@launch
                }

                tmpFile.writeText(uniqueFiles.joinToString("\n"))
                logger.debug(tag, uniqueFiles.joinToString("\n"))
            }

            val copyCmd = arrayOf(
                    "rsync", "--files-from", tmpFile.absolutePath, "-e", "ssh -i $PRIVATE_KEY_PATH", "$SSH_USER@$remoteAddress:$FILE_SYSTEM_PATH/", "$FILE_SYSTEM_PATH/"
            )

            tmpFile.delete()

            logger.debug(tag, copyCmd.joinToString(" "))

            val proc = ProcessBuilder(*copyCmd).start()

            // block until file has been copied
            proc.waitFor()

            if (proc.exitValue() > 0) {
                BufferedReader(InputStreamReader(lsProc.errorStream)).use { reader ->
                    logger.warn(tag, "Errors while syncing files: ${reader.readLines().joinToString("\n")}")
                }
            } else {
                val files = File(FILE_SYSTEM_PATH)
                logger.info(tag, "copied '${files.list().size}' files from $remoteAddress")
            }
        }
    }

    fun saveAddressListToDisk(addresses: List<String>) {
        launch(CommonPool) {
            // Write given list of addresses to disk
            // Note: existing servers file will be overwritten if present
            File(SERVERS_FILE_PATH).writeText(addresses.joinToString("\n") + "\n")
        }
    }
}