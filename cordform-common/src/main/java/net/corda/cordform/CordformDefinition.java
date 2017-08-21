package net.corda.cordform;

import org.bouncycastle.asn1.x500.X500Name;
import java.nio.file.Path;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public abstract class CordformDefinition {
    public final Path driverDirectory;
    public final ArrayList<Consumer<? super CordformNode>> nodeConfigurers = new ArrayList<>();
    public final X500Name networkMapNodeName;
    private Map<CordformNode, KeyPair> map;

    public CordformDefinition(Path driverDirectory, X500Name networkMapNodeName) {
        this.driverDirectory = driverDirectory;
        this.networkMapNodeName = networkMapNodeName;
    }

    public void addNode(Consumer<? super CordformNode> configurer) {
        nodeConfigurers.add(configurer);
    }

    private static Map<CordformNode, KeyPair> generateKeys(List<CordformNode> nodes) {
        Map<CordformNode, KeyPair> map = new HashMap<>();
        for (CordformNode node : nodes) {
            map.put(node, Crypto.generateKeyPair());
        }
        return map;
    }

    public static void writeKeys(List<CordformNode> nodes) {
        Map<CordformNode, KeyPair> map = generateKeys(nodes);
        NodeInfoSerializer.toDisk(map);
    }

    /**
     * Make arbitrary changes to the node directories before they are started.
     * @param nodes List of nodes which are going to be deployed, this list
     *              will usually be the result of applying the {@link #nodeConfigurers}
     *              to a list of freshly created nodes.
     */
    public abstract void setup(List<CordformNode> nodes);
}
