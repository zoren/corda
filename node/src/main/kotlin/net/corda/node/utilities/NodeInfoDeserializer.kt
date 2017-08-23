package net.corda.node.utilities

import com.typesafe.config.*
import net.corda.cordform.NAME_CONFIG_KEY
import net.corda.cordform.P2P_ADDRESS
import net.corda.cordform.PUBLIC_KEY_CONFIG_KEY
import net.corda.core.crypto.*
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.node.NodeInfo
import net.corda.core.node.ServiceEntry
import net.corda.core.node.WorldMapLocation
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.NonEmptySet
import net.corda.core.utilities.parseNetworkHostAndPort
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.util.encoders.Base64
import java.io.File
import java.security.cert.Certificate
import java.security.cert.CertificateFactory
import java.util.logging.Logger


object NodeInfoDeserializer {

    /**
     * Read a file written by [NodeInfoSerializer.toDisk] and creates a [NodeInfo]
     * @param file the file to read.
     * @param clientCA the certificate and key pair corresponding to the root CA.
     *                 since this function is meant for tests this will be X509Utilities.CORDA_CLIENT_CA
     * @param clientCertificatePath the "rest" of the certificate path under which the new node will be 'attached'
     *
     */
    fun fromDisk(file: File, clientCA : CertificateAndKeyPair, clientCertificatePath: Array<out Certificate>) :
            NodeInfo {

        val config = ConfigFactory.parseFile(file, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))

        val name: X500Name = X500Name(config.getString(NAME_CONFIG_KEY))
        val p2pAddress = config.getString(P2P_ADDRESS)
        val publicKey = Crypto.decodePublicKey(Base64.decode(config.getString(PUBLIC_KEY_CONFIG_KEY)))

        val thatCert = X509Utilities.createCertificate(
                CertificateType.IDENTITY, clientCA.certificate, clientCA.keyPair, name, publicKey)

        val certPath = CertificateFactory.getInstance("X509").generateCertPath(listOf(thatCert.cert) + clientCertificatePath)

        val partyAndCertificate = PartyAndCertificate(certPath)
        val addresses: List<NetworkHostAndPort> = listOf(p2pAddress.parseNetworkHostAndPort())
        val legalIdentityAndCert = partyAndCertificate //TODO This field will be removed in future PR which gets rid of services.
        val legalIdentitiesAndCerts: NonEmptySet<PartyAndCertificate> = NonEmptySet.of(legalIdentityAndCert)
        val platformVersion: Int = 0
        val advertisedServices: List<ServiceEntry> = emptyList()
        val worldMapLocation: WorldMapLocation = WorldMapLocation()

        return NodeInfo(addresses, legalIdentityAndCert, legalIdentitiesAndCerts, platformVersion, advertisedServices, worldMapLocation)
    }
}
