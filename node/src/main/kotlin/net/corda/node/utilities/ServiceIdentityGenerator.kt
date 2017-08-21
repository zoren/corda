package net.corda.node.utilities

import net.corda.cordform.CordformNode
import net.corda.core.crypto.composite.CompositeKey
import net.corda.core.crypto.generateKeyPair
import net.corda.core.identity.Party
import net.corda.core.node.services.ServiceInfo
import net.corda.core.utilities.loggerFor
import net.corda.core.utilities.trace
import org.bouncycastle.asn1.x500.X500Name
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyPair



object ServiceIdentityGenerator {
    private val log = loggerFor<ServiceIdentityGenerator>()

    fun generateKeys(nodes: List<CordformNode>) : Map<CordformNode, KeyPair> {
        return nodes.associateBy({ it }, { generateKeyPair() })
    }

    /**
     * Generates signing key pairs and a common distributed service identity for a set of nodes.
     * The key pairs and the group identity get serialized to disk in the corresponding node directories.
     * This method should be called *before* any of the nodes are started.
     *
     * @param dirs List of node directories to place the generated identity and key pairs in.
     * @param keys Map from dirs to the corresponding [KeyPair]
     * @param serviceId The service id of the distributed service.
     * @param serviceName The legal name of the distributed service.
     * @param threshold The threshold for the generated group [CompositeKey].
     */
    // TODO: This needs to write out to the key store, not just files on disk
    fun generateToDisk(dirs: List<Path>,
                       keys: Map<Path, KeyPair>,
                       serviceId: String,
                       serviceName: X500Name,
                       threshold: Int = 1): Unit {
        log.trace { "Generating a group identity \"$serviceName\" for nodes: ${dirs.joinToString()}" }
        val notaryKey = CompositeKey.Builder().addKeys(keys.values.map { it.public }).build(threshold)
        // Avoid adding complexity! This class is a hack that needs to stay runnable in the gradle environment.
        val privateKeyFile = "$serviceId-private-key"
        val publicKeyFile = "$serviceId-public"
        val compositeKeyFile = "$serviceId-composite-key"
        dirs.forEach { dir ->
            val keyPair = keys[dir]!!
            Files.createDirectories(dir)
            Files.write(dir.resolve(compositeKeyFile), notaryKey.encoded)
            Files.write(dir.resolve(privateKeyFile), keyPair.private.encoded)
            Files.write(dir.resolve(publicKeyFile), keyPair.public.encoded)
        }
    }
}
