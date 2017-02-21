package net.corda.client

import com.google.common.util.concurrent.Futures
import net.corda.client.model.NodeMonitorModel
import net.corda.client.model.ProgressTrackingEvent
import net.corda.core.bufferUntilSubscribed
import net.corda.core.contracts.DOLLARS
import net.corda.core.contracts.issuedBy
import net.corda.core.flows.StateMachineRunId
import net.corda.core.getOrThrow
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.messaging.StateMachineUpdate
import net.corda.core.messaging.startFlow
import net.corda.core.node.services.NetworkMapCache
import net.corda.core.node.services.ServiceInfo
import net.corda.core.node.services.StateMachineTransactionMapping
import net.corda.core.node.services.Vault
import net.corda.core.serialization.OpaqueBytes
import net.corda.core.transactions.SignedTransaction
import net.corda.flows.CashExitFlow
import net.corda.flows.CashIssueFlow
import net.corda.flows.CashPaymentFlow
import net.corda.node.internal.Node
import net.corda.node.services.User
import net.corda.node.services.config.configureTestSSL
import net.corda.node.services.network.NetworkMapService
import net.corda.node.services.startFlowPermission
import net.corda.node.services.transactions.SimpleNotaryService
import net.corda.testing.expect
import net.corda.testing.expectEvents
import net.corda.testing.node.NodeBasedTest
import net.corda.testing.sequence
import org.assertj.core.api.Assertions.assertThat
import org.junit.Before
import org.junit.Test
import rx.Observable

class NodeMonitorModelTest : NodeBasedTest() {
    private lateinit var aliceNode: Node
    private lateinit var notaryNode: Node

    private lateinit var rpc: CordaRPCOps
    private lateinit var stateMachineTransactionMapping: Observable<StateMachineTransactionMapping>
    private lateinit var stateMachineUpdates: Observable<StateMachineUpdate>
    private lateinit var progressTracking: Observable<ProgressTrackingEvent>
    private lateinit var transactions: Observable<SignedTransaction>
    private lateinit var vaultUpdates: Observable<Vault.Update>
    private lateinit var networkMapUpdates: Observable<NetworkMapCache.MapChange>

    @Before
    fun `setUp`() {
        val cashUser = User("user1", "test", permissions = setOf(
                startFlowPermission<CashIssueFlow>(),
                startFlowPermission<CashPaymentFlow>(),
                startFlowPermission<CashExitFlow>())
        )
        val nodes = Futures.allAsList(
                startNode("Alice", rpcUsers = listOf(cashUser)),
                startNode("Notary", advertisedServices = setOf(ServiceInfo(SimpleNotaryService.type)))
        ).getOrThrow()

        aliceNode = nodes[0]
        notaryNode = nodes[1]
        val monitor = NodeMonitorModel()

        stateMachineTransactionMapping = monitor.stateMachineTransactionMapping.bufferUntilSubscribed()
        stateMachineUpdates = monitor.stateMachineUpdates.bufferUntilSubscribed()
        progressTracking = monitor.progressTracking.bufferUntilSubscribed()
        transactions = monitor.transactions.bufferUntilSubscribed()
        vaultUpdates = monitor.vaultUpdates.bufferUntilSubscribed()
        networkMapUpdates = monitor.networkMap.bufferUntilSubscribed()

        monitor.register(aliceNode.configuration.artemisAddress, configureTestSSL(), cashUser.username, cashUser.password)
        rpc = monitor.proxyObservable.value!!
    }

    @Test
    fun `network map update`() {
        Futures.allAsList(
                startNode("Bob"),
                startNode("Charlie")
        ).getOrThrow()
        networkMapUpdates
                .filter { !it.node.advertisedServices.any { it.info.type.isNotary() } }
                .filter { !it.node.advertisedServices.any { it.info.type == NetworkMapService.type } }
                .expectEvents(isStrict = false) {
                    sequence(
                            // TODO : Add test for remove when driver DSL support individual node shutdown.
                            expect { output: NetworkMapCache.MapChange ->
                                assertThat(output.node.legalIdentity.name).isEqualTo("Alice")
                            },
                            expect { output: NetworkMapCache.MapChange ->
                                assertThat(output.node.legalIdentity.name).isEqualTo("Bob")
                            },
                            expect { output: NetworkMapCache.MapChange ->
                                assertThat(output.node.legalIdentity.name).isEqualTo("Charlie")
                            }
                    )
                }
    }

    @Test
    fun `cash issue works end to end`() {
        rpc.startFlow(::CashIssueFlow,
                100.DOLLARS,
                OpaqueBytes.of(1),
                aliceNode.info.legalIdentity,
                notaryNode.info.notaryIdentity
        )

        vaultUpdates.expectEvents(isStrict = false) {
            sequence(
                    // SNAPSHOT
                    expect { output: Vault.Update ->
                        assertThat(output.consumed).isEmpty()
                        assertThat(output.produced).isEmpty()
                    },
                    // ISSUE
                    expect { output: Vault.Update ->
                        assertThat(output.consumed).isEmpty()
                        assertThat(output.produced).hasSize(1)
                    }
            )
        }
    }

    @Test
    fun `cash issue and move`() {
        rpc.startFlow(::CashIssueFlow,
                100.DOLLARS,
                OpaqueBytes.of(1),
                aliceNode.info.legalIdentity,
                notaryNode.info.notaryIdentity
        ).returnValue.getOrThrow()

        rpc.startFlow(::CashPaymentFlow,
                100.DOLLARS.issuedBy(aliceNode.info.legalIdentity.ref(1)),
                aliceNode.info.legalIdentity
        )

        var issueSmId: StateMachineRunId? = null
        var moveSmId: StateMachineRunId? = null
        var issueTx: SignedTransaction? = null
        var moveTx: SignedTransaction? = null
        stateMachineUpdates.expectEvents {
            sequence(
                    // ISSUE
                    expect { add: StateMachineUpdate.Added ->
                        issueSmId = add.id
                    },
                    expect { remove: StateMachineUpdate.Removed ->
                        assertThat(remove.id).isEqualTo(issueSmId)
                    },
                    // MOVE
                    expect { add: StateMachineUpdate.Added ->
                        moveSmId = add.id
                    },
                    expect { remove: StateMachineUpdate.Removed ->
                        assertThat(remove.id).isEqualTo(moveSmId)
                    }
            )
        }

        transactions.expectEvents {
            sequence(
                    // ISSUE
                    expect { stx ->
                        assertThat(stx.tx.inputs).isEmpty()
                        assertThat(stx.tx.outputs).hasSize(1)
                        val signaturePubKeys = stx.sigs.map { it.by }.toSet()
                        // Only Alice signed
                        val aliceKey = aliceNode.info.legalIdentity.owningKey
                        assertThat(signaturePubKeys.size).isLessThanOrEqualTo(aliceKey.keys.size)
                        assertThat(aliceKey.isFulfilledBy(signaturePubKeys)).isTrue()
                        issueTx = stx
                    },
                    // MOVE
                    expect { stx ->
                        assertThat(stx.tx.inputs).hasSize(1)
                        assertThat(stx.tx.outputs).hasSize(1)
                        val signaturePubKeys = stx.sigs.map { it.by }.toSet()
                        // Alice and Notary signed
                        assertThat(aliceNode.info.legalIdentity.owningKey.isFulfilledBy(signaturePubKeys)).isTrue()
                        assertThat(notaryNode.info.notaryIdentity.owningKey.isFulfilledBy(signaturePubKeys)).isTrue()
                        moveTx = stx
                    }
            )
        }

        vaultUpdates.expectEvents {
            sequence(
                    // SNAPSHOT
                    expect { output: Vault.Update ->
                        assertThat(output.consumed).isEmpty()
                        assertThat(output.produced).isEmpty()
                    },
                    // ISSUE
                    expect { update ->
                        assertThat(update.consumed).isEmpty()
                        assertThat(update.produced).hasSize(1)
                    },
                    // MOVE
                    expect { update ->
                        assertThat(update.consumed).hasSize(1)
                        assertThat(update.produced).hasSize(1)
                    }
            )
        }

        stateMachineTransactionMapping.expectEvents {
            sequence(
                    // ISSUE
                    expect { mapping ->
                        assertThat(mapping.stateMachineRunId).isEqualTo(issueSmId)
                        assertThat(mapping.transactionId).isEqualTo(issueTx!!.id)
                    },
                    // MOVE
                    expect { mapping ->
                        assertThat(mapping.stateMachineRunId).isEqualTo(moveSmId)
                        assertThat(mapping.transactionId).isEqualTo(moveTx!!.id)
                    }
            )
        }
    }
}
