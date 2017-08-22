package net.corda.notarydemo

import net.corda.cordform.CordformNode
import net.corda.cordform.NodeInfo
import net.corda.cordform.NodeInfoSerializer
import net.corda.core.node.services.ServiceInfo
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.node.utilities.ServiceIdentityGenerator
import org.bouncycastle.asn1.x500.X500Name

/**
 * @return whether a CordaformNode runs a notary.
 */
private fun CordformNode.isNotary() : Boolean {
    return this.advertisedServices
            .map { service -> ServiceInfo.parse(service) }
            .any { serviceInfo -> serviceInfo.type.isNotary() }
}

/**
 * All-in one setup for notary clusters.
 * Creates notary identity keys for notaries and creates a composite key with those.
 * Then the notary identity keys are written to disk to the notary nodes folders.
 *
 * Then each node identity is generated for non-notaries and written to disk (both keys).
 *
 * Then for each non-notary node PLUS the notary cluster a "NodeInfo" is created and that is
 * serialized in the additional-nodes-folder for the above.
 * In the case of the notary cluster members they each get a
 */
fun notaryClusterSetup(nodes: List<CordformNode>,
                       clusterName : X500Name,
                       clusterAddress : List<NetworkHostAndPort>,
                       advertisedServiceId: String,
                       replicas :Int = 1) {
    val (notaries, nonNotaries) = nodes.partition { it.isNotary() }
    val notaryKeys = ServiceIdentityGenerator.generateKeys(notaries)
    // Generate and write to disk the notary notary keys and generate the composite key.
    val compositeKey = ServiceIdentityGenerator.generateToDisk(
            notaries.map { it.nodeDir.toPath() },
            notaryKeys.mapKeys { kv -> kv.key.nodeDir.toPath() },
            advertisedServiceId, clusterName, replicas)

    val notaryClusterNode = NodeInfo(clusterName.toString(), notaries.map { it -> it.nodeDir.toPath() },
            "notaryCluster", clusterAddress.toString(), compositeKey)

    val nodeInfos = NodeInfoSerializer.generateKeysFor(nonNotaries)

    // Write each node identity to disk.
    NodeInfoSerializer.writeKeyPairs(nodeInfos, "identity")

    // Populate the additional-node-infos folder.
    NodeInfoSerializer.serializeNodeInfo(nodeInfos.keys + notaryClusterNode)
}