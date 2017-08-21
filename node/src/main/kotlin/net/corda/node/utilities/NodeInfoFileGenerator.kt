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
import java.security.cert.CertPathBuilder
import java.security.cert.Certificate
import java.security.cert.CertificateFactory

internal val NAME_CONFIG_KEY = "name"
internal val PUBLIC_KEY_CONFIG_KEY = "public-key"

object NodeInfoFileGenerator {
    private val logger = loggerFor<NodeInfoFileGenerator>()

    private fun makeConfig(cordformNode: CordformNode, key: PublicKey) : Config {
        // Note key.toBase58String() requires corda serialization to have been bootstrapped.
        return configOf(
                NAME_CONFIG_KEY to cordformNode.name,
                PUBLIC_KEY_CONFIG_KEY to String(Base64.encode(key.encoded)))
    }

    /**
     *
     */
    fun fromDisk(file: File, clientCA : CertificateAndKeyPair, ccp : Array<out Certificate>) : NodeInfo {

        val appConfig = ConfigFactory.parseFile(file)
        val name: X500Name = X500Name(appConfig.getString(NAME_CONFIG_KEY))
        val publicKey = Crypto.decodePublicKey(Base64.decode(appConfig.getString(PUBLIC_KEY_CONFIG_KEY)))

        val thatCert = X509Utilities.createCertificate(
                CertificateType.IDENTITY, clientCA.certificate, clientCA.keyPair, name, publicKey)


        val certPath = CertificateFactory.getInstance("X509").generateCertPath(listOf(thatCert.cert) + ccp)

        val partyAndCertificate = PartyAndCertificate(certPath)
        val addresses: List<NetworkHostAndPort> = listOf()
        val legalIdentityAndCert = partyAndCertificate //TODO This field will be removed in future PR which gets rid of services.
        val legalIdentitiesAndCerts: NonEmptySet<PartyAndCertificate> = NonEmptySet.of(legalIdentityAndCert)
        val platformVersion: Int = 0
        val advertisedServices: List<ServiceEntry> = emptyList()
        val worldMapLocation: WorldMapLocation? = null

        return NodeInfo(addresses, legalIdentityAndCert, legalIdentitiesAndCerts, platformVersion, advertisedServices, worldMapLocation)
    }
}
