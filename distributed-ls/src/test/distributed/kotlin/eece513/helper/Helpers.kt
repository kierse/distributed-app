import java.io.File

fun pathToAssets(): String {
    return arrayOf(System.getProperty("user.dir"), "servers.txt")
            .joinToString(File.separator)
}