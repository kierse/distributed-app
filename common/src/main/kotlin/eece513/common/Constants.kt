package eece513.common

const val CONNECTION_PORT = 6971

const val PROMPT_WINDOW = 60L

val ABSOLUTE_ROOT_PATH = System.getProperty("user.dir")

val FILE_SYSTEM_PATH = "$ABSOLUTE_ROOT_PATH/files"

const val FILE_SYSTEM_SEPARATOR = "-"

val SERVERS_FILE_PATH = "$ABSOLUTE_ROOT_PATH/servers.txt"

val PRIVATE_KEY_PATH = "$ABSOLUTE_ROOT_PATH/.ssh/server-to-server"

const val SSH_USER = "ec2-user"

const val MESSAGE_HEADER_SIZE = 2 // in bytes

