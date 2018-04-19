package eece513.fsClient

import eece513.common.CONNECTION_PORT
import eece513.common.Logger
import eece513.common.PRIVATE_KEY_PATH
import eece513.common.SSH_USER
import eece513.common.mapper.ConnectionPurposeMapper
import eece513.common.model.ConnectionPurpose
import eece513.fsClient.mapper.FsCommandByteMapper
import eece513.fsClient.mapper.FsResponseObjectMapper
import eece513.common.model.FsCommand
import eece513.common.model.FsResponse
import eece513.fsClient.message.MessageBuilder
import eece513.fsClient.message.MessageReader
import kotlinx.coroutines.experimental.*
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess

class FsClient(
        private val remoteAddresses: List<InetAddress>,
        private val purposeMapper: ConnectionPurposeMapper,
        private val commandMapper: FsCommandByteMapper,
        private val responseMapper: FsResponseObjectMapper,
        private val messageBuilder: MessageBuilder,
        private val messageReader: MessageReader,
        private val logger: Logger
) {
    private val tag = FsClient::class.java.simpleName

    fun executeCommand(command: FsCommand) = when (command) {
        is FsCommand.FsPut -> put(command)
        is FsCommand.FsGet -> get(command)
        is FsCommand.FsRemove -> remove(command)

        else -> throw IllegalArgumentException("unknown command type!")
    }

    private fun put(command: FsCommand.FsPut) {
        val response = processRequestAndAnswer(ConnectionPurpose.ClientPut(), command)

        logger.info(tag, "Received response: $response")

        when (response) {
            is FsResponse.PutAck -> {
                pushFileToNodes(response, command)
            }

            is FsResponse.PutAckWithPrompt -> {
                if (!confirmOrTimeout(command)) {
                    logger.warn(tag, "Aborting! Prompt timeout!")
                    return
                }

                pushFileToNodes(response, command)
            }

            else -> throw IllegalArgumentException("unsupported response type! $response")
        }
    }

    private fun get(command: FsCommand.FsGet) {
        val response = processRequestAndAnswer(ConnectionPurpose.ClientGet(), command)
        logger.debug(tag, "received: $response")

        when (response) {
            is FsResponse.GetResponse -> {
                fetchFileAndSave(response, command)
            }

            is FsResponse.UnknownFile -> {
                print("Requested file '${command.remoteFile}' is unknown!")
                exitProcess(1)
            }

            else -> throw IllegalArgumentException("unsupported response type! $response")
        }
    }

    private fun remove(command: FsCommand.FsRemove) {
        logger.info(tag, "Requesting removal of ${command.remoteFile}")
        processRequestAndAnswer(ConnectionPurpose.ClientRemove(), command)
        logger.info(tag, "Successfully removed all versions of ${command.remoteFile}")
    }

    private fun processRequestAndAnswer(purpose: ConnectionPurpose, command: FsCommand): FsResponse {
       Socket().use { socket ->
           val address = InetSocketAddress(remoteAddresses.first(), CONNECTION_PORT)
           socket.connect(address)
           logger.info(tag, "opened connection to ${remoteAddresses.first()}")

           sendPurposeAndCommand(socket, purpose, command)

           return getResponse(socket)
       }
    }

    private fun sendPurposeAndCommand(socket: Socket, purpose: ConnectionPurpose, command: FsCommand) {
        val stream = socket.getOutputStream()

        // send purpose
        logger.debug(tag, "sending purpose: $purpose")
        val purposeMsg = messageBuilder.build(purposeMapper.toByteArray(purpose))
        stream.write(purposeMsg)

        // send command
        logger.debug(tag, "sending command: $command")
        val commandMsg = messageBuilder.build(commandMapper.toByteArray(command))
        stream.write(commandMsg)
    }

    private fun getResponse(socket: Socket): FsResponse {
        val stream = socket.getInputStream()

        // read and parse response
        return responseMapper.toObject(messageReader.read(stream))
    }

    private fun fetchFileAndSave(response: FsResponse.GetResponse, command: FsCommand.FsGet) {
        val cmd = if (File(response.pathToFile).exists()) {
            logger.info(tag, "copying file from local node")
            arrayOf("cp", response.pathToFile, command.localFile)
        } else {
            val remoteAddress = remoteAddresses.first().hostName
            logger.info(tag, "copying file from $remoteAddress")

            arrayOf(
                    "rsync", "-e", "ssh -i $PRIVATE_KEY_PATH", "$SSH_USER@$remoteAddress:${response.pathToFile}", command.localFile
            )
        }

        logger.debug(tag, cmd.joinToString(" "))

        val proc = ProcessBuilder(*cmd).start()

        // block until file has been copied
        proc.waitFor()

        if (proc.exitValue() > 0) {
            BufferedReader(InputStreamReader(proc.errorStream)).use { reader ->
                logger.warn(tag, "Errors while fetching file: ${reader.readLines().joinToString("\n")}")
            }
            return
        }

        logger.info(tag, "File ${command.remoteFile} successfully written to ${command.localFile}")
    }

    private fun confirmOrTimeout(command: FsCommand.FsPut) = runBlocking {
        val result = withTimeoutOrNull(INPUT_TIMEOUT, TimeUnit.SECONDS) {
            val job = async(CommonPool) {
                val result: Boolean?
                while (true) {
                    print("'${command.remoteFile}' has been updated in the last 60 seconds. Do you want to overwrite? [Y/n] ")
                    val response = readLine() ?: continue

                    if (arrayOf("Y", "y").contains(response)) {
                        result = true
                        break
                    } else if (arrayOf("N", "n").contains(response)) {
                        result = false
                        break
                    }
                }

                result
            }

            job.await()
        }

        return@runBlocking result ?: false
    }

    private fun pushFileToNodes(response: FsResponse.PutResponse, command: FsCommand.FsPut) = runBlocking {
        val context = newFixedThreadPoolContext(3, "pushFileToNode")

        val jobs = remoteAddresses.map { remoteAddress ->
            logger.debug(tag, "pushing file to ${remoteAddress.hostName}")

            launch(context) {
                val file = File(response.pathToFile)

                val tempPath = "/tmp/${file.name}"
                val scpCommand = arrayOf(
                        "rsync", "-e", "ssh -i $PRIVATE_KEY_PATH", command.localFile, "$SSH_USER@${remoteAddress.hostName}:$tempPath"
                )
                //"scp -i $PRIVATE_KEY_PATH \"${command.localFile}\" $SSH_USER@$remoteAddress:\"$tempPath\""
                logger.debug(tag, scpCommand.joinToString(" "))

                val copy = ProcessBuilder(*scpCommand).start()

                // block until file has been copied
                copy.waitFor()

                if (copy.exitValue() > 0) {
                    BufferedReader(InputStreamReader(copy.errorStream)).use { reader ->
                        logger.warn(tag, "Errors while pushing file: ${reader.readLines().joinToString("\n")}")
                    }

                    return@launch
                }

                val sshCommand = arrayOf(
                        "ssh", "-i", PRIVATE_KEY_PATH, "$SSH_USER@${remoteAddress.hostName}", "mv", tempPath, response.pathToFile
                )
                logger.debug(tag, sshCommand.joinToString(" "))

                val move = ProcessBuilder(*sshCommand).start()

                move.waitFor()

                if (move.exitValue() > 0) {
                    BufferedReader(InputStreamReader(move.errorStream)).use { reader ->
                        logger.warn(tag, "Error while renaming file: ${reader.readLines().joinToString("\n")}")
                    }

                    return@launch
                }

                logger.info(tag, "File ${command.localFile} successfully added to filesystem as ${command.remoteFile}")
            }
        }

        logger.debug(tag, "Blocking...")

        // block and wait for all jobs to finish
        jobs.forEach { it.join() }

        logger.debug(tag, "Done!")
    }
}