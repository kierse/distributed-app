package eece513.common.util

import eece513.common.FILE_SYSTEM_PATH
import eece513.common.FILE_SYSTEM_SEPARATOR
import java.io.File

fun unescapeFileName(escapedName: String): String {
    return escapedName.replace("+", "/")
}

fun escapeFileName(name: String): String {
    return name.replace("/", "+")
}

fun getLocalFiles(): List<String> {
    val fileNames = mutableSetOf<String>()

    return File(FILE_SYSTEM_PATH)
            .listFiles()
            .map { file ->
                unescapeFileName(file.name.substringBeforeLast(FILE_SYSTEM_SEPARATOR))
            }
            .filter { name ->
                fileNames.add(name)
            }
}

fun getLatestVersion(remoteName: String): File? {
    return File(FILE_SYSTEM_PATH)
            .listFiles()
            .filter { file ->
                file.name.startsWith(escapeFileName(remoteName))
            }
            .sortedBy { it.name }
            .lastOrNull()
}

