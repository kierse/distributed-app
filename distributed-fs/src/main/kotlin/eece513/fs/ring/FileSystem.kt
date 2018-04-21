package eece513.fs.ring

import eece513.common.*
import eece513.common.util.escapeFileName
import eece513.common.util.unescapeFileName
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.launch
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.net.InetSocketAddress
import java.time.Instant

class FileSystem internal /* for testing */ constructor(
        private val path: String,
        private val serversPath: String,
        private val logger: Logger,
        startingData: MutableMap<String, MutableList<String>>
) {
    private val tag = FileSystem::class.java.simpleName

    private val remoteFileToTimestamp = startingData

    constructor(path: String, serversPath: String, logger: Logger): this(
            path, serversPath, logger, mutableMapOf<String, MutableList<String>>()
    )

    fun initialize(selfAddress: String) {
        saveAddressListToDisk(listOf(selfAddress))

        // make sure filesystem directory exists
        File(path).mkdirs()

        // make sure filesystem is empty
        // NOTE: this will also delete the servers.txt file!!
        File(path)
                .listFiles()
                .forEach { file ->
                    file.delete()
                }
    }

    fun removeFile(remoteName: String) {
        launch(CommonPool) {
            File(path)
                    .listFiles()
                    .filter { file ->
                        file.name.startsWith(escapeFileName(remoteName))
                    }
                    .forEach { file ->
                        // Note: assuming we are the file owner here
                        logger.debug(tag, "removing file: $path/${file.name}")
                        file.delete()
                    }
        }
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
        val name = pathToFile.substringBeforeLast(FILE_SYSTEM_SEPARATOR)
        val number = pathToFile.substringAfterLast(FILE_SYSTEM_SEPARATOR)

        return Pair(
                unescapeFileName(name),
                Instant.ofEpochMilli(number.toLong())
        )
    }

    fun buildEncodedFileName(remoteFileName: String, timestamp: Instant): File {
        return File("$path/${escapeFileName(remoteFileName)}$FILE_SYSTEM_SEPARATOR${timestamp.toEpochMilli()}")
    }

    private fun decodeHumanReadableFileName(pathToFile: String): String {
        val encodedFileName = pathToFile.substringAfterLast("/")
        return unescapeFileName(encodedFileName.substringBeforeLast(FILE_SYSTEM_SEPARATOR))
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
            val lsCmd = arrayOf("ssh", "-i", PRIVATE_KEY_PATH, "$SSH_USER@$remoteAddress", "ls", "-1r", path)

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
                    "rsync", "--files-from", tmpFile.absolutePath, "-e", "ssh -i $PRIVATE_KEY_PATH", "$SSH_USER@$remoteAddress:$path/", "$path/"
            )

            logger.debug(tag, copyCmd.joinToString(" "))

            val proc = ProcessBuilder(*copyCmd).start()

            // block until file has been copied
            proc.waitFor()

            if (proc.exitValue() > 0) {
                BufferedReader(InputStreamReader(proc.errorStream)).use { reader ->
                    logger.warn(tag, "Errors while syncing files: ${reader.readLines().joinToString("\n")}")
                }
            } else {
                val files = File(path)
                logger.info(tag, "copied '${files.list().size}' files from $remoteAddress")
            }

            // cleanup
            tmpFile.delete()
        }
    }

    fun saveAddressListToDisk(addresses: List<String>) {
        launch(CommonPool) {
            // Write given list of addresses to disk
            // Note: existing servers file will be overwritten if present
            File(serversPath).writeText(addresses.joinToString("\n") + "\n")
        }
    }
}