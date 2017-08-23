package net.corda.cordform

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigRenderOptions
import org.bouncycastle.util.encoders.Base64
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyPair
import java.security.PublicKey


const val NAME_CONFIG_KEY = "name"
const val PUBLIC_KEY_CONFIG_KEY = "public-key"
const val NODE_INFO_FOLDER = "additional-node-infos"
const val P2P_ADDRESS = "p2pAddress"

internal val renderOptions = ConfigRenderOptions.defaults().setOriginComments(false).setComments(false).setJson(false)


/**
 * A representation of a node containing all the data needed to re-create a
 * corda NodeInfo.
 * @param name the X500 name of the node
 * @param paths list of paths where this node will be installed. This is a single element list unless this is a cluster
 * @param dirName a representation of the name suitable for being used in a path or filename, {@link CordformNode#getDirName}
 * @param p2pAddress a string representation of the address of the node, like "localhost:1234"
 * @param publicKey the public key for the node.
 * TODO consider whether it makes sense to store the private key here as well, note that clusters don't have one.
 */
data class CordformNodeInfo(val name: String, val paths: List<Path>, val dirName: String, val p2pAddress: String, val publicKey: PublicKey)

object NodeInfoSerializer {
    private fun configOf(vararg pairs: Pair<String, Any?>): Config = ConfigFactory.parseMap(mapOf(*pairs))

    private fun makeConfig(cordformNodeInfo: CordformNodeInfo): Config {
        // Note key.toBase58String() requires corda serialization to have been bootstrapped.
        return configOf(
                NAME_CONFIG_KEY to cordformNodeInfo.name,
                P2P_ADDRESS to cordformNodeInfo.p2pAddress,
                PUBLIC_KEY_CONFIG_KEY to String(Base64.encode(cordformNodeInfo.publicKey.encoded)))
    }

    /**
     * @param paths list of paths in which to write the key pairs
     * @param pathsToKeys a mapping from Path to KeyPair. Each path above must be contained in this map.
     * Note: this function identifies nodes by paths (and not by some node class) because i
     */
    fun writeKeyPairs(paths: List<Path>, pathsToKeys: Map<Path, KeyPair>, serviceId: String) {
        val privateKeyFile = "$serviceId-private-key"
        val publicKeyFile = "$serviceId-public"
        paths.forEach { dir ->
            val keyPair = pathsToKeys[dir]!!
            dir.toFile().mkdirs()
            Files.write(dir.resolve(privateKeyFile), keyPair.private.encoded)
            Files.write(dir.resolve(publicKeyFile), keyPair.public.encoded)
        }
    }

    /**
     * Writes a set of keyPairs for a set of nodes.
     * @param nodes
     */
    @JvmStatic
    fun writeKeyPairs(nodes: Map<CordformNodeInfo, KeyPair>, serviceId: String) {
        require(nodes.keys.map { it.paths.size }.all { size -> size == 1 })
        // These NodeInfos have a KeyPair, hence they have a single path.
        // Multiple paths are used for clusters which don't have a whole KeyPair associated with,
        // they just have a compositeKey which is represented as a PublicKey.
        writeKeyPairs(nodes.keys.map { it -> it.paths.first() },
                nodes.mapKeys { it -> it.key.paths.first() }, serviceId)
    }

    /**
     * Write a .conf file 'representing' a NodeInfo for each node in each node folder (n^2)
     * Each file contains the name of the node, plus the public key needed to sign it.
     *
     * Also for each node write to disk thier identity keyPair (n files will be written)
     *
     */
    @JvmStatic
    fun serializeNodeInfo(
            nodes: Collection<CordformNodeInfo>) {
        val configMap = nodes.associate { node -> node to makeConfig(node) }

        for (node in nodes) {
            for (path in node.paths) {
                val nodePath = path.resolve(NODE_INFO_FOLDER)
                nodePath.toFile().mkdirs()

                for ((otherNode, config) in configMap) {
                    // Avoid writing a node own NodeInfo in its folder.
                    if (otherNode.dirName == node.dirName) continue
                    val file = (nodePath.resolve(otherNode.dirName + ".conf")).toFile()
                    file.writeText(config.root().render(renderOptions))
                }
            }
        }
    }

    private fun CordformNode.toNodeInfo(publicKey: PublicKey) = CordformNodeInfo(this.name,
            listOf(this.nodeDir.toPath()), this.dirName,
            this.config.getString("p2pAddress"), publicKey)

    /**
     * Transforms a collection of CordfromNodes into NodeInfos, this process also generates the keyPair
     * for each node.
     * @param nodes a list of [CordformNode]s
     * @return a list of [CordformNodeInfo]s
     */
    @JvmStatic
    fun generateKeysFor(nodes: List<CordformNode>): Map<CordformNodeInfo, KeyPair> {
        return nodes.associate { node ->
            val keyPair = Crypto.generateKeyPair()
            node.toNodeInfo(keyPair.public) to keyPair
        }
    }
}