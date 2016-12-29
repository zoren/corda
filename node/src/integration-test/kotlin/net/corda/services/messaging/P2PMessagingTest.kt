package net.corda.services.messaging

import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import net.corda.core.*
import net.corda.core.messaging.*
import net.corda.core.node.services.DEFAULT_SESSION_ID
import net.corda.core.node.services.ServiceInfo
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.flows.ServiceRequestMessage
import net.corda.flows.sendRequest
import net.corda.node.internal.Node
import net.corda.node.services.network.NetworkMapService
import net.corda.node.services.transactions.RaftValidatingNotaryService
import net.corda.node.services.transactions.SimpleNotaryService
import net.corda.node.utilities.ServiceIdentityGenerator
import net.corda.testing.freeLocalHostAndPort
import net.corda.testing.node.NodeBasedTest
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.Test
import java.util.*
import java.util.concurrent.TimeoutException

class P2PMessagingTest : NodeBasedTest() {
    @Test
    fun `communicating with self`() {
        val alice = startNode("Alice").getOrThrow()
        alice.respondWith("self")
        val received = alice.receiveFrom(alice.net.myAddress).getOrThrow()
        assertThat(received).isEqualTo("self")
    }

    @Test
    fun `network map will work after restart`() {
        fun startNodes() = Futures.allAsList(startNode("NodeA"), startNode("NodeB"), startNode("Notary"))

        val startUpDuration = elapsedTime { startNodes().getOrThrow() }
        // Start the network map a second time - this will restore message queues from the journal.
        // This will hang and fail prior the fix. https://github.com/corda/corda/issues/37
        stopAllNodes()
        startNodes().getOrThrow(timeout = startUpDuration.multipliedBy(3))
    }

    @Test
    fun `send message to self before the network map is available and receive after`() {
        val generalTopic = javaClass.name
        val networkMapTopic = NetworkMapService.FETCH_FLOW_TOPIC

        val alice = startNode("Alice", startNetworkMap = false).getOrThrow()
        val generalReceive = alice.net.onNext<String>(generalTopic, DEFAULT_SESSION_ID)
        val networkMapReceive = alice.net.onNext<String>(networkMapTopic, DEFAULT_SESSION_ID)

        alice.net.send(generalTopic, DEFAULT_SESSION_ID, "first msg", alice.net.myAddress)
        alice.net.send(networkMapTopic, DEFAULT_SESSION_ID, "second msg", alice.net.myAddress)

        assertThat(networkMapReceive.getOrThrow()).isEqualTo("second msg")
        assertThatExceptionOfType(TimeoutException::class.java).isThrownBy {
            generalReceive.getOrThrow(2.seconds)
        }
        startNetworkMapNode()
        assertThat(generalReceive.getOrThrow()).isEqualTo("first msg")
    }

    // https://github.com/corda/corda/issues/71
    @Test
    fun `communicating with a service running on the network map node`() {
        startNetworkMapNode(advertisedServices = setOf(ServiceInfo(SimpleNotaryService.type)))
        networkMapNode.respondWith("Hello")
        val alice = startNode("Alice").getOrThrow()
        val serviceAddress = alice.services.networkMapCache.run {
            alice.net.getAddressOfParty(getPartyInfo(getAnyNotary()!!)!!)
        }
        val received = alice.receiveFrom(serviceAddress).getOrThrow(10.seconds)
        assertThat(received).isEqualTo("Hello")
    }

    @Test
    fun `communicating with a distributed service which the network map node is part of`() {
        val serviceName = "DistributedService"

        val root = tempFolder.root.toPath()
        ServiceIdentityGenerator.generateToDisk(
                listOf(root / networkMapNodeName, root / "Service Node 2"),
                RaftValidatingNotaryService.type.id,
                serviceName)

        val distributedService = ServiceInfo(RaftValidatingNotaryService.type, serviceName)
        val notaryClusterAddress = freeLocalHostAndPort()
        startNetworkMapNode(
                advertisedServices = setOf(distributedService),
                configOverrides = mapOf("notaryNodeAddress" to notaryClusterAddress.toString()))
        val (serviceNode2, alice) = Futures.allAsList(
            startNode(
                "Service Node 2",
                advertisedServices = setOf(distributedService),
                configOverrides = mapOf(
                        "notaryNodeAddress" to freeLocalHostAndPort().toString(),
                        "notaryClusterAddresses" to listOf(notaryClusterAddress.toString()))),
            startNode("Alice")
        ).getOrThrow()

        // Setup each node in the distributed service to return back it's Party so that we can know which node is being used
        val serviceNodes = listOf(networkMapNode, serviceNode2)
        serviceNodes.forEach {
            it.respondWith(it.info.legalIdentity)
        }

        val serviceAddress = alice.services.networkMapCache.run {
            alice.net.getAddressOfParty(getPartyInfo(getNotary(serviceName)!!)!!)
        }
        val participatingParties = HashSet<Any>()
        // Try several times so that we can be fairly sure that any node not participating is not due to Artemis' selection strategy
        for (it in 1..serviceNodes.size*3) {
            participatingParties += alice.receiveFrom(serviceAddress).getOrThrow(10.seconds)
            if (participatingParties.size == serviceNodes.size) {
                break
            }
        }
        assertThat(participatingParties).containsOnlyElementsOf(serviceNodes.map { it.info.legalIdentity })
    }

//    private fun Node.respondWithFlowMessage(message: Any) {
//        services.registerFlowInitiator(ReceiveFlow::class) { SendFlow(it, message) }
//    }
//
//    private fun Node.receiveFromParty(party: Party): ListenableFuture<Any> {
//        return services.startFlow(ReceiveFlow(party)).resultFuture
//    }
//
//    private class SendFlow(val otherParty: Party, val payload: Any) : FlowLogic<Unit>() {
//        @Suspendable
//        override fun call() = send(otherParty, payload)
//    }
//
//    private class ReceiveFlow(val otherParty: Party) : FlowLogic<Any>() {
//        @Suspendable
//        override fun call() = receive<Any>(otherParty).unwrap { it }
//    }

    private fun Node.respondWith(message: Any) {
        net.addMessageHandler(javaClass.name, DEFAULT_SESSION_ID) { netMessage, reg ->
            val request = netMessage.data.deserialize<TestRequest>()
            val response = net.createMessage(javaClass.name, request.sessionID, message.serialize().bytes)
            net.send(response, request.replyTo)
        }
    }

    private fun Node.receiveFrom(target: MessageRecipients): ListenableFuture<Any> {
        val request = TestRequest(replyTo = net.myAddress)
        return net.sendRequest<Any>(javaClass.name, request, target)
    }

    private data class TestRequest(override val sessionID: Long = random63BitValue(),
                                   override val replyTo: SingleMessageRecipient) : ServiceRequestMessage
}