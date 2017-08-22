package net.corda.cordform

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigRenderOptions
import net.corda.cordform.CordformNode
import org.bouncycastle.util.encoders.Base64
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyPair
import java.security.PublicKey
import java.util.HashMap


const val NAME_CONFIG_KEY = "name"
const val PUBLIC_KEY_CONFIG_KEY = "public-key"
const val NODE_INFO_FOLDER = "additional-node-infos"
const val P2P_ADDRESS = "p2pAddress"

internal val renderOptions = ConfigRenderOptions.defaults().setOriginComments(false).setComments(false).setJson(false)


/**
 * A representation of a node containing all the data needed to re-create a
 * corda NodeInfo.
 */
data class NodeInfo(val name: String, val path: List<Path>, val dirName: String, val p2paddress: String,
                    val publicKey: PublicKey)

object NodeInfoSerializer {
    private fun configOf(vararg pairs: Pair<String, Any?>): Config = ConfigFactory.parseMap(mapOf(*pairs))

    private fun makeConfig(nodeInfo: NodeInfo): Config {
        // Note key.toBase58String() requires corda serialization to have been bootstrapped.
        return configOf(
                NAME_CONFIG_KEY to nodeInfo.name,
                P2P_ADDRESS to nodeInfo.p2paddress,
                PUBLIC_KEY_CONFIG_KEY to String(Base64.encode(nodeInfo.publicKey.encoded)))
    }

    /**
     * @param dirs list of paths in which to write the key pairs
     * @param keys a mapping from Path to KeyPair
     */
    fun writeKeyPairs(dirs: List<Path>, keys: Map<Path, KeyPair>, serviceId: String) {
        val privateKeyFile = "$serviceId-private-key"
        val publicKeyFile = "$serviceId-public"
        dirs.forEach { dir ->
            val keyPair = keys[dir]!!
            dir.toFile().mkdirs()
            Files.write(dir.resolve(privateKeyFile), keyPair.private.encoded)
            Files.write(dir.resolve(publicKeyFile), keyPair.public.encoded)
        }
    }

    @JvmStatic
    fun writeKeyPairs(nodes: Map<NodeInfo, KeyPair>, serviceId: String) {
        require(nodes.keys.map { it.path.size }.all { size -> size == 1 })
        // These NodeInfos have a KeyPair, hence they have a single path.
        // Multiple paths are used for clusters which don't have a whole KeyPair associated with,
        // they just have a compositeKey which is represented as a PublicKey.
        writeKeyPairs(nodes.keys.map { it -> it.path.first() },
                nodes.mapKeys { it -> it.key.path.first() }, serviceId)
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
            nodes: Collection<NodeInfo>) {
        val configMap = nodes.associate { node -> node to makeConfig(node) }

        for (node in nodes) {
            for (path in node.path) {
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

    internal fun CordformNode.toNodeInfo(publicKey: PublicKey) = NodeInfo(this.name,
            listOf(this.nodeDir.toPath()), this.dirName,
            this.config.getString("p2pAddress"), publicKey)

    /**
     * Transforms a collection of CordfromNodes into NodeInfos, this process also generates the keyPair
     * for each node.
     * @param nodes a list of [CordformNode]s
     * @return a list of [NodeInfo]s
     */
    @JvmStatic
    fun generateKeysFor(nodes: List<CordformNode>): Map<NodeInfo, KeyPair> {
        return nodes.associate { node ->
            val keyPair = Crypto.generateKeyPair()
            node.toNodeInfo(keyPair.public) to keyPair
        }
    }
}