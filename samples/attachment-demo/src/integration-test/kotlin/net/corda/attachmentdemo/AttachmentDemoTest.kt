package net.corda.attachmentdemo

import com.google.common.util.concurrent.Futures
import net.corda.core.getOrThrow
import net.corda.core.node.services.ServiceInfo
import net.corda.node.services.User
import net.corda.node.services.transactions.SimpleNotaryService
import net.corda.testing.node.NodeBasedTest
import org.junit.Test

class AttachmentDemoTest : NodeBasedTest() {
    @Test
    fun `runs attachment demo`() {
        val demoUser = listOf(User("demo", "demo", setOf("StartFlow.net.corda.flows.FinalityFlow")))
        val (nodeA, nodeB) = Futures.allAsList(
                startNode("Bank A", rpcUsers = demoUser),
                startNode("Bank B", rpcUsers = demoUser),
                startNode("Notary", setOf(ServiceInfo(SimpleNotaryService.type)))
        ).getOrThrow()

        val senderFuture = rpcClientTo(nodeA).run {
            start(demoUser[0].username, demoUser[0].password)
            sender(proxy())
        }

        val recipientFuture = rpcClientTo(nodeB).run {
            start(demoUser[0].username, demoUser[0].password)
            recipient(proxy())
        }

        Futures.allAsList(senderFuture, recipientFuture).getOrThrow()
    }
}
