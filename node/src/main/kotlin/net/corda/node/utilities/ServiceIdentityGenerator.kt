package net.corda.node.utilities

import net.corda.cordform.CordformNode
import net.corda.cordform.NodeInfoSerializer
import net.corda.core.crypto.composite.CompositeKey
import net.corda.core.crypto.generateKeyPair
import net.corda.core.utilities.loggerFor
import net.corda.core.utilities.trace
import org.bouncycastle.asn1.x500.X500Name
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyPair
import java.security.PublicKey


object ServiceIdentityGenerator {
    private val log = loggerFor<ServiceIdentityGenerator>()

    fun generateKeys(nodes: List<CordformNode>) : Map<CordformNode, KeyPair> {
        return nodes.associate { it to generateKeyPair() }
    }

    /**
     * Generates signing key pairs and a common distributed service identity for a set of nodes.
     * The key pairs and the group identity get serialized to disk in the corresponding node directories.
     * This method should be called *before* any of the nodes are started.
     *
     * @param dirs List of node directories to place and key pairs (public and private) in.
     * @param keys Map from dirs to the corresponding [KeyPair]
     * @param serviceId The service id of the distributed service.
     * @param serviceName The legal name of the distributed service.
     * @param threshold The threshold for the generated group [CompositeKey].
     */
    // Implementation note: this function identifies nodes using Paths since it's used by both
    // cordformation and nodebased tests which respectively use CordformationNode and Node to represent nodes.
    // TODO: This needs to write out to the key store, not just files on disk
    fun generateToDisk(dirs: List<Path>,
                       keys: Map<Path, KeyPair>,
                       serviceId: String,
                       serviceName: X500Name,
                       threshold: Int = 1) : PublicKey {
        // Avoid adding complexity! This class is a hack that needs to stay runnable in the gradle environment.
        NodeInfoSerializer.writeKeyPairs(dirs, keys, serviceId)
        return generateAndWriteCompositeKey(keys, dirs, serviceName, serviceId, threshold)
    }

    fun generateAndWriteCompositeKey(keys: Map<Path, KeyPair>,
                                     notariesDirs: List<Path>,
                                     serviceName: X500Name,
                                     serviceId: String,
                                     threshold: Int = 1) : PublicKey {
        log.trace { "Generating a group identity \"$serviceName\" for nodes: ${notariesDirs.joinToString()}" }
        val notaryKey = CompositeKey.Builder()
                .addKeys(notariesDirs.map { keys[it]!!.public })
                .build(threshold)
        val compositeKeyFile = "$serviceId-composite-key"
        for (dir in notariesDirs) {
            Files.createDirectories(dir)
            Files.write(dir.resolve(compositeKeyFile), notaryKey.encoded)
        }
        return notaryKey
    }


}
