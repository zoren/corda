package net.corda.cordform;

import net.i2p.crypto.eddsa.EdDSASecurityProvider;
import net.i2p.crypto.eddsa.spec.EdDSANamedCurveTable;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.jcajce.provider.util.AsymmetricKeyInfoConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.IOException;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.security.cert.Certificate;

/**
 * This code is a lightweight extract of Crypto.kt
 */
public class Crypto {

    private static final String ALGORITHM_NAME = "1.3.101.112";

    private Crypto() {}

    private static final Provider BOUNCY_CASTLE_PROVIDER = getBouncyCastleProvider();

    private static BouncyCastleProvider getBouncyCastleProvider() {
        BouncyCastleProvider bouncyCastleProvider = new BouncyCastleProvider();
        bouncyCastleProvider.putAll(new EdDSASecurityProvider());

        bouncyCastleProvider.addKeyInfoConverter(
                new ASN1ObjectIdentifier(ALGORITHM_NAME),
                new AsymmetricKeyInfoConverter() {
                    @Override
                    public PublicKey generatePublic(SubjectPublicKeyInfo keyInfo) throws IOException {
                        if (keyInfo == null) return null;
                        try {
                            KeyFactory keyFactory = KeyFactory.getInstance(ALGORITHM_NAME, BOUNCY_CASTLE_PROVIDER);
                            return keyFactory.generatePublic(new X509EncodedKeySpec(keyInfo.getEncoded()));
                        } catch (InvalidKeySpecException | NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }
                        return null;
                    }

                    @Override
                    public PrivateKey generatePrivate(PrivateKeyInfo keyInfo) throws IOException {
                        if (keyInfo == null) return null;
                        try {
                            KeyFactory keyFactory = KeyFactory.getInstance(ALGORITHM_NAME, BOUNCY_CASTLE_PROVIDER);
                            return keyFactory.generatePrivate(new PKCS8EncodedKeySpec(keyInfo.getEncoded()));
                        } catch (InvalidKeySpecException | NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }
                        return null;
                    }
                }
        );
        return bouncyCastleProvider;
    }

    public static KeyPair generateKeyPair() {
        try {
            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(ALGORITHM_NAME, BOUNCY_CASTLE_PROVIDER);
            keyPairGenerator.initialize(EdDSANamedCurveTable.getByName("ED25519"), new SecureRandom());
            return keyPairGenerator.generateKeyPair();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
