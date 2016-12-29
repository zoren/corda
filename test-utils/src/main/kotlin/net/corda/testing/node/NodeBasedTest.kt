package net.corda.testing.node

import com.google.common.net.HostAndPort
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import net.corda.core.createDirectories
import net.corda.core.div
import net.corda.core.map
import net.corda.core.node.services.ServiceInfo
import net.corda.node.internal.Node
import net.corda.node.services.User
import net.corda.node.services.config.ConfigHelper
import net.corda.node.services.config.FullNodeConfiguration
import net.corda.testing.freeLocalHostAndPort
import org.junit.After
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import java.util.*
import kotlin.concurrent.thread

/**
 * Extend this class if you need to run nodes in a test. You could use the driver DSL but it's extremely slow for testing
 * purposes. Use the DSL if you need to run the nodes in separate processes otherwise this class will suffice.
 */
// TODO Some of the logic here duplicates what's in the driver
abstract class NodeBasedTest {
    @Rule
    @JvmField
    val tempFolder = TemporaryFolder()

    private val nodes = ArrayList<Node>()
    private var _networkMapNode: Node? = null

    val networkMapNodeName = "Network Map"
    val networkMapNodeAddress = freeLocalHostAndPort()
    val networkMapNode: Node get() = _networkMapNode ?: startNetworkMapNode()

    /**
     * Stops the network map node and all the nodes started by [startNode]. This is called automatically after each test
     * but can also be called manually within a test.
     */
    @After
    fun stopAllNodes() {
        nodes.forEach(Node::stop)
        nodes.clear()
        _networkMapNode = null
    }

    /**
     * You can use this method to start the network map node in a more customised manner. Otherwise it
     * will automatically be started with the default parameters.
     */
    fun startNetworkMapNode(advertisedServices: Set<ServiceInfo> = emptySet(),
                            rpcUsers: List<User> = emptyList(),
                            configOverrides: Map<String, Any> = emptyMap()): Node {
        check(_networkMapNode == null)
        return startNodeInternal(networkMapNodeName, networkMapNodeAddress, advertisedServices, rpcUsers, configOverrides).apply {
            _networkMapNode = this
        }
    }

    /**
     * Starts a node with the given name. If the [startNetworkMap] is true (which is the default) and the network map node
     * isn't already running then it will be started automatically.
     */
    fun startNode(legalName: String,
                  address: HostAndPort = freeLocalHostAndPort(),
                  advertisedServices: Set<ServiceInfo> = emptySet(),
                  rpcUsers: List<User> = emptyList(),
                  configOverrides: Map<String, Any> = emptyMap(),
                  startNetworkMap: Boolean = true): ListenableFuture<Node> {
        if (startNetworkMap && _networkMapNode == null) {
            startNetworkMapNode()
        }
        val node = startNodeInternal(
                legalName,
                address,
                advertisedServices,
                rpcUsers,
                mapOf(
                        "networkMapService" to mapOf(
                                "address" to networkMapNodeAddress.toString(),
                                "legalName" to networkMapNodeName
                        )
                ) + configOverrides
        )
        return if (_networkMapNode == null) Futures.immediateFuture(node) else node.networkMapRegistrationFuture.map { node }
    }

    private fun startNodeInternal(legalName: String,
                                  address: HostAndPort,
                                  advertisedServices: Set<ServiceInfo>,
                                  rpcUsers: List<User>,
                                  configOverrides: Map<String, Any>): Node {
        val config = ConfigHelper.loadConfig(
                baseDirectoryPath = (tempFolder.root.toPath() / legalName).createDirectories(),
                allowMissingConfig = true,
                configOverrides = mapOf(
                        "myLegalName" to legalName,
                        "artemisAddress" to address.toString(),
                        "extraAdvertisedServiceIds" to advertisedServices.joinToString(","),
                        "rpcUsers" to rpcUsers.map {
                            mapOf(
                                    "user" to it.username,
                                    "password" to it.password,
                                    "permissions" to it.permissions
                            )
                        }
                ) + configOverrides
        )

        val node = FullNodeConfiguration(config).createNode()
        node.start()
        nodes += node
        thread(name = legalName) {
            node.run()
        }
        return node
    }
}