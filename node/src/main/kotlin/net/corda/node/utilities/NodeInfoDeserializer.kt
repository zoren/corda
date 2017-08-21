package net.corda.node.utilities

import com.typesafe.config.*
import net.corda.core.crypto.*
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.node.NodeInfo
import net.corda.core.node.ServiceEntry
import net.corda.core.node.WorldMapLocation
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.NonEmptySet
import net.corda.core.utilities.loggerFor
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.util.encoders.Base64
import java.io.File
import java.security.cert.Certificate
import java.security.cert.CertificateFactory
import java.util.logging.Logger

internal val NAME_CONFIG_KEY = "name"
internal val PUBLIC_KEY_CONFIG_KEY = "public-key"
val NODE_INFO_FOLDER = "additional-node-infos"


object NodeInfoDeserializer {
    /**
     *
     */
    fun fromDisk(file: File, clientCA : CertificateAndKeyPair, clientCertificatePath: Array<out Certificate>) :
            NodeInfo {

        val appConfig = ConfigFactory.parseFile(file, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))

        val name: X500Name = X500Name(appConfig.getString(NAME_CONFIG_KEY))
        val publicKey = Crypto.decodePublicKey(Base64.decode(appConfig.getString(PUBLIC_KEY_CONFIG_KEY)))

        val thatCert = X509Utilities.createCertificate(
                CertificateType.IDENTITY, clientCA.certificate, clientCA.keyPair, name, publicKey)

        val certPath = CertificateFactory.getInstance("X509").generateCertPath(listOf(thatCert.cert) + clientCertificatePath)

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
