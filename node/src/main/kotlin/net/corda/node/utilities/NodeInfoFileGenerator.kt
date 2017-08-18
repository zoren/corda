package net.corda.node.utilities

import com.typesafe.config.*
import net.corda.cordform.CordformNode
import net.corda.core.crypto.*
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.internal.div
import net.corda.core.node.NodeInfo
import net.corda.core.node.ServiceEntry
import net.corda.core.node.WorldMapLocation
import net.corda.core.node.services.KeyManagementService
import net.corda.core.serialization.SerializedBytes
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.utilities.ByteSequence
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.NonEmptySet
import net.corda.core.utilities.loggerFor
import net.corda.node.services.config.configOf
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.cert.X509CertificateHolder
import org.bouncycastle.util.encoders.Base64
import java.io.File
import java.nio.file.Path
import java.security.KeyPair
import java.security.PublicKey
import java.security.cert.CertPath

val NAME_CONFIG_KEY = "name"
val PUBLIC_KEY_CONFIG_KEY = "public-key"

object NodeInfoFileGenerator {
    private val logger = loggerFor<NodeInfoFileGenerator>()


    fun makeConfig(cordformNode: CordformNode, key: PublicKey) : Config {
        // Note key.toBase58String() requires corda serialization to have been bootstrapped.
        return configOf(
                NAME_CONFIG_KEY to cordformNode.name,
                PUBLIC_KEY_CONFIG_KEY to String(Base64.encode(key.encoded)))
    }

    fun toDisk(nodes: List<CordformNode>, keys : Map<CordformNode, KeyPair>) {
        val configMap = LinkedHashMap<CordformNode, Config>()
        nodes.forEach { node ->
            configMap.put(node, makeConfig(node, keys[node]!!.public))
        }
        
        nodes.forEach { node ->
            val certPath = node.nodeDir.toPath() / "additional-node-infos"
            certPath.toFile().mkdirs()

            configMap.forEach { (otherNode,config) ->
                val file = (certPath / otherNode.relativeDir).toFile()
                file.writeText(config.root().render(ConfigRenderOptions.defaults()))
            }
        }


    }

    fun fromDisk(file: File, certPath: CertPath) : NodeInfo {
        val appConfig = ConfigFactory.parseFile(file)
        val name: X500Name = X500Name(appConfig.getString(NAME_CONFIG_KEY))
        val owningKey = Crypto.decodePublicKey(Base64.decode(appConfig.getString(PUBLIC_KEY_CONFIG_KEY)))

        val x509 = X509CertificateHolder(certPath.certificates.first().encoded)
        val partyAndCertificate = PartyAndCertificate(name, owningKey ,x509, certPath)
        val addresses: List<NetworkHostAndPort> = listOf()
        val legalIdentityAndCert = partyAndCertificate //TODO This field will be removed in future PR which gets rid of services.
        val legalIdentitiesAndCerts: NonEmptySet<PartyAndCertificate> = NonEmptySet.of(legalIdentityAndCert)
        val platformVersion: Int = 0
        val advertisedServices: List<ServiceEntry> = emptyList()
        val worldMapLocation: WorldMapLocation? = null

        return NodeInfo(addresses, legalIdentityAndCert, legalIdentitiesAndCerts, platformVersion, advertisedServices, worldMapLocation)
    }
}




fun loadFromFile(file : File) : NodeInfo {
    val signedData: SignedData<NodeInfo> = ByteSequence.of(file.readBytes()).deserialize()
    // TODO: check the signature
    return signedData.verified()
}

fun saveToFile(path : Path, nodeInfo: NodeInfo, keyManager: KeyManagementService, publicKey: PublicKey) {
    path.toFile().mkdirs()
    val sb : SerializedBytes<NodeInfo> = nodeInfo.serialize()
    val regSig = keyManager.sign(sb.bytes, publicKey)
    val sd : SignedData<NodeInfo> = SignedData(sb, regSig)
    val file : File =  (path / sd.hashCode().toString()).toFile()
    file.writeBytes(sd.serialize().bytes)
}