package net.corda.cordform;

import static java.util.Collections.emptyList;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.BCStyle;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CordformNode implements NodeDefinition {
    protected static final String DEFAULT_HOST = "localhost";

    /**
     * Name of the node.
     */
    private String name;

    public String getName() {
        return name;
    }

    /**
     * Set the name of the node.
     *
     * @param name The node name.
     */
    public void name(String name) {
        this.name = name;
        config = config.withValue("myLegalName", ConfigValueFactory.fromAnyRef(name));
    }

    /**
     * Path in which the node will be installed.
     */
    private File nodeDir;

    public void nodeDir(File nodeDir) {
        this.nodeDir = nodeDir;
    }

    public File getNodeDir() {
        return nodeDir;
    }

    /**
     * A list of advertised services ID strings.
     */
    public List<String> advertisedServices = emptyList();

    /**
     * If running a Raft notary cluster, the address of at least one node in the cluster, or leave blank to start a new cluster.
     * If running a BFT notary cluster, the addresses of all nodes in the cluster.
     */
    public List<String> notaryClusterAddresses = emptyList();
    /**
     * Set the RPC users for this node. This configuration block allows arbitrary configuration.
     * The recommended current structure is:
     * [[['username': "username_here", 'password': "password_here", 'permissions': ["permissions_here"]]]
     * The above is a list to a map of keys to values using Groovy map and list shorthands.
     *
     * Incorrect configurations will not cause a DSL error.
     */
    public List<Map<String, Object>> rpcUsers = emptyList();

    protected Config config = ConfigFactory.empty();

    public Config getConfig() {
        return config;
    }

    /**
     * @return a string representation of the name of this node suitable for being used as a path component or file name.
     *      Usually this is used to build a path to install a node.
     */
    public String getDirName() {
        String dirName;
        try {
            X500Name x500Name = new X500Name(getName());
            dirName = x500Name.getRDNs(BCStyle.CN)[0].getFirst().getValue().toString();
        } catch (IllegalArgumentException ignore) {
            // Can't parse as an X500 name, use the full string
            dirName = getName();
        }
        return dirName.replaceAll("\\s","");
    }

    /**
     * Set the Artemis P2P port for this node.
     *
     * @param p2pPort The Artemis messaging queue port.
     */
    public void p2pPort(Integer p2pPort) {
        config = config.withValue("p2pAddress", ConfigValueFactory.fromAnyRef(DEFAULT_HOST + ':' + p2pPort));
    }

    /**
     * Set the Artemis RPC port for this node.
     *
     * @param rpcPort The Artemis RPC queue port.
     */
    public void rpcPort(Integer rpcPort) {
        config = config.withValue("rpcAddress", ConfigValueFactory.fromAnyRef(DEFAULT_HOST + ':' + rpcPort));
    }

    /**
     * Set the port which to bind the Copycat (Raft) node to.
     *
     * @param notaryPort The Raft port.
     */
    public void notaryNodePort(Integer notaryPort) {
        config = config.withValue("notaryNodeAddress", ConfigValueFactory.fromAnyRef(DEFAULT_HOST + ':' + notaryPort));
    }

    /**
     * @param id The (0-based) BFT replica ID.
     */
    public void bftReplicaId(Integer id) {
        config = config.withValue("bftSMaRt", ConfigValueFactory.fromMap(Collections.singletonMap("replicaId", id)));
    }
}
