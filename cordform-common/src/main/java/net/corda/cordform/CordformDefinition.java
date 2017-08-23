package net.corda.cordform;

import org.bouncycastle.asn1.x500.X500Name;
import java.nio.file.Path;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toMap;

public abstract class CordformDefinition {
    public final Path driverDirectory;
    public final ArrayList<Consumer<? super CordformNode>> nodeConfigurers = new ArrayList<>();
    public final X500Name networkMapNodeName;

    public CordformDefinition(Path driverDirectory, X500Name networkMapNodeName) {
        this.driverDirectory = driverDirectory;
        this.networkMapNodeName = networkMapNodeName;
    }

    public void addNode(Consumer<? super CordformNode> configurer) {
        nodeConfigurers.add(configurer);
    }

    /**
     * Generates and writes identities keyPairs for each node to disk.
     * @param nodes the nodes.
     */
    public static void generateAndWriteIdentityKeys(List<CordformNode> nodes) {
        Map<CordformNodeInfo, KeyPair> nodeInfos = NodeInfoSerializer.generateKeysFor(nodes);

        // Write each node identity to disk.
        NodeInfoSerializer.writeKeyPairs(nodeInfos, "identity");

        // Populate the additional-node-infos folder inside each "path".
        NodeInfoSerializer.serializeNodeInfo(nodeInfos.keySet());
    }

    /**
     * Make arbitrary changes to the node directories before they are started.
     * @param nodes List of nodes which are going to be deployed, this list
     *              will usually be the result of applying the {@link #nodeConfigurers}
     *              to a list of freshly created nodes.
     */
    public abstract void setup(List<CordformNode> nodes);
}
