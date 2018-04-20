package eece513.common.util

import eece513.common.FILE_SYSTEM_PATH
import java.io.File

fun unescapeFileName(escapedName: String): String {
    return escapedName.replace("+", "/")
}

fun escapeFileName(name: String): String {
    return name.replace("/", "+")
}

fun getLocalFiles(): List<File> {
    val fileNames = mutableSetOf<String>()

    return File(FILE_SYSTEM_PATH)
            .listFiles()
            .sortedBy { it.name }
            .reversed()
            .filter { file ->
                fileNames.add(file.name)
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

