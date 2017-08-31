package net.corda.vega

import net.corda.core.node.services.ServiceInfo
import net.corda.core.utilities.getOrThrow
import net.corda.testing.DUMMY_BANK_A
import net.corda.testing.DUMMY_BANK_B
import net.corda.testing.DUMMY_BANK_C
import net.corda.testing.DUMMY_NOTARY
import net.corda.node.services.transactions.SimpleNotaryService
import net.corda.testing.driver.driver
import java.util.concurrent.CompletableFuture.allOf

/**
 * Sample main used for running within an IDE. Starts 4 nodes (A, B, C and Notary/Controller) as an alternative to running via gradle
 * This does not start any tests but has the nodes running in preparation for a live web demo or to receive commands
 * via the web api.
 */
fun main(args: Array<String>) {
    driver(dsl = {
        startNode(DUMMY_NOTARY.name, setOf(ServiceInfo(SimpleNotaryService.type)))
        val nodeAFuture = startNode(DUMMY_BANK_A.name).toCompletableFuture()
        val nodeBFuture = startNode(DUMMY_BANK_B.name).toCompletableFuture()
        val nodeCFuture = startNode(DUMMY_BANK_C.name).toCompletableFuture()
        allOf(nodeAFuture, nodeBFuture, nodeCFuture).getOrThrow()
        val (nodeA, nodeB, nodeC) = listOf(nodeAFuture, nodeBFuture, nodeCFuture).map { it.getOrThrow() }

        startWebserver(nodeA)
        startWebserver(nodeB)
        startWebserver(nodeC)

        waitForAllNodesToFinish()
    }, isDebug = true)
}
