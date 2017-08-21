package net.corda.cordform

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigRenderOptions
import org.bouncycastle.util.encoders.Base64
import java.security.KeyPair
import java.security.PublicKey


internal val NAME_CONFIG_KEY = "name"
internal val PUBLIC_KEY_CONFIG_KEY = "public-key"
internal val NODE_INFO_FOLDER = "additional-node-infos"

internal val renderOptions =  ConfigRenderOptions.defaults().setOriginComments(false).setComments(false).setJson(false)

object NodeInfoSerializer {
    private fun configOf(vararg pairs: Pair<String, Any?>): Config = ConfigFactory.parseMap(mapOf(*pairs))

    private fun makeConfig(cordformNode: CordformNode, key: PublicKey): Config {
        // Note key.toBase58String() requires corda serialization to have been bootstrapped.
        return configOf(
                NAME_CONFIG_KEY to cordformNode.name,
                PUBLIC_KEY_CONFIG_KEY to String(Base64.encode(key.encoded)))
    }

    /**
     * @param nodes
     * @param keys
     */
    @JvmStatic
    fun toDisk(keys: Map<CordformNode, KeyPair>) {
        val nodes = keys.keys.toList()
        val configMap = nodes.associateBy({ it }, { node ->
            makeConfig(node, keys[node]!!.public)
        })

        nodes.forEach { node ->
            val nodePath = node.nodeDir.toPath().resolve(NODE_INFO_FOLDER)
            nodePath.toFile().mkdirs()

            configMap.forEach { (otherNode, config) ->
                val file = (nodePath.resolve(otherNode.relativeDir + ".conf")).toFile()
                file.writeText(config.root().render(renderOptions))
            }
        }
    }
}