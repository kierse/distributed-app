package eece513.fsClient

import eece513.common.SERVERS_FILE_PATH
import eece513.common.TinyLogWrapper
import eece513.common.mapper.ConnectionPurposeMapper
import eece513.common.util.FileIO
import eece513.fsClient.mapper.FsCommandByteMapper
import eece513.fsClient.mapper.FsResponseObjectMapper
import eece513.common.model.FsCommand
import eece513.fsClient.message.MessageBuilder
import eece513.fsClient.message.MessageReader
import kotlin.system.exitProcess

fun main(args: Array<String>) {
//    val logger = TinyLogWrapper(LOG_LOCATION)
    val logger = TinyLogWrapper()

    val remoteAddresses = FileIO().ReadLinesAsInetAddress(SERVERS_FILE_PATH)
    val purposeMapper = ConnectionPurposeMapper()
    val commandMapper = FsCommandByteMapper()
    val responseMapper = FsResponseObjectMapper()
    val messageBuilder = MessageBuilder()
    val messageReader = MessageReader(logger)

    try {
        val client = FsClient(remoteAddresses, purposeMapper, commandMapper, responseMapper, messageBuilder, messageReader, logger)
        client.executeCommand(FsCommand.from(args))
    } catch (e: IllegalArgumentException) {
        print("error: ${e.message}")
        exitProcess(-1)
    }
}
