package eece513.common.model

sealed class FsCommand {
    companion object {
        fun from(args: Array<String>): FsCommand {
            if (args.size < 2) throw IllegalArgumentException("not enough args!")

            if (args.size == 2) {
                if (!arrayOf("REMOVE", "remove").contains(args[0])) throw IllegalArgumentException("unknown command: ${args[0]}")
                if (args[1].isBlank()) throw IllegalArgumentException("must provide remote file name!")
                return FsRemove(args[1])
            }

            if (arrayOf("GET", "get").contains(args[0])) {
                if (args[2].isBlank()) throw IllegalArgumentException("must provide local file path!")
                return FsGet(args[1], args[2])
            }

            if (arrayOf("PUT", "put").contains(args[0])) {
                if (args[2].isBlank()) throw IllegalArgumentException("must provide remote file name!")
                return FsPut(args[1], args[2])
            }

            throw IllegalArgumentException("unknown/unrecognized command")
        }

    }

    data class FsGet(val remoteFile: String, val localFile: String) : FsCommand()
    data class FsPut(val localFile: String, val remoteFile: String) : FsCommand()
    data class FsRemove(val remoteFile: String) : FsCommand()
    data class PutConfirm(val pathToFile: String) : FsCommand()
}