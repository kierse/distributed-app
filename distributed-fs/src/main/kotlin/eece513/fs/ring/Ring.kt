package eece513.fs.ring

import eece513.common.model.FsResponse
import eece513.fs.channel.*
import eece513.common.model.Node

interface Ring {
    val rebuildRing: Boolean

    fun addSuccessor(successor: Node)
    fun removeSuccessor(successor: Node)

    fun sendJoinRequest(channel: SendClusterActionChannel): Boolean
    fun readMembershipList(channel: ReadMembershipListChannel): Boolean

    fun leave()

    fun processJoinRequest(channel: ReadClusterActionChannel): Boolean
    fun sendMembershipList(channel: SendMembershipListChannel): Boolean

    fun sendIdentity(channel: SendClusterActionChannel): Boolean
    fun readIdentity(channel: ReadClusterActionChannel): Node?

    fun sendActionsToSuccessor(successor: Node, channel: BufferedSendActionChannel)
    fun processActionsFromPredecessor(predecessor: Node, channel: ReadActionChannel)

    fun processHeartbeat(channel: ReadHeartbeatChannel)
    fun processMissedHeartbeats(channel: MissedHeartbeatChannel)

    fun dropPredecessor(predecessor: Node)

    fun getPredecessors(): List<Node>
    fun getSuccessors(): List<Node>

    fun processFileRemove(channel: ReadFsCommandChannel): Boolean
    fun sendFileRemoveResponse(channel: SendFsResponseChannel): Boolean

    fun processFileGet(channel: ReadFsCommandChannel): FsResponse?
    fun sendFileGetResponse(channel: FsResponseStateChannel): Boolean

    fun processFilePut(channel: ReadFsCommandChannel): FsResponse?
    fun processFilePutConfirm(channel: ReadFsCommandChannel): Boolean
    fun sendFilePutResponse(channel: FsResponseStateChannel): Boolean
}