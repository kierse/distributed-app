package eece513.fs

import eece513.fs.Actions
import org.junit.Test
import java.time.Instant

class ProtoMeasurements {
    @Test
    fun request_size() {
        val request = Actions.Request.newBuilder()
                .setType(Actions.Request.Type.JOIN)
                .setTimestamp(createTimestamp())
                .setHostName("127.0.0.1")
                .setPort(6969)
                .build()
                .toByteArray()

        println("request: ${request.size}")
    }

    @Test
    fun membership_list_size() {
        val request = Actions.MembershipList.newBuilder()
                .addNode(createNode())
                .addNode(createNode())
                .addNode(createNode())
                .addNode(createNode())
                .build()
                .toByteArray()

        println("membership list: ${request.size}")
    }

    @Test
    fun membership_size() {
        println("membership: ${createNode().toByteArray().size}")
    }

    private fun createTimestamp(): Actions.Timestamp {
        val now = Instant.now()
        return Actions.Timestamp.newBuilder()
                .setSecondsSinceEpoch(now.epochSecond)
                .setNanoSeconds(now.nano)
                .build()
    }

    private fun createNode(): Actions.Membership {
        return Actions.Membership.newBuilder()
                .setTimestamp(createTimestamp())
                .setHostName("127.0.0.1")
                .setPort(6969)
                .build()
    }
}