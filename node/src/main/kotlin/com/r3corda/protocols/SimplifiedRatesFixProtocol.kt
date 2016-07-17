package com.r3corda.protocols

import co.paralleluniverse.fibers.Suspendable
import com.r3corda.core.contracts.Fix
import com.r3corda.core.contracts.FixOf
import com.r3corda.core.contracts.TransactionBuilder
import com.r3corda.core.contracts.WireTransaction
import com.r3corda.core.crypto.DigitalSignature
import com.r3corda.core.crypto.Party
import com.r3corda.core.random63BitValue
import com.r3corda.core.utilities.UntrustworthyData
import com.r3corda.core.utilities.suggestInterestRateAnnouncementTimeWindow
import com.r3corda.node.services.statemachine.SimplifiedProtocolStateMachine
import com.r3corda.node.services.statemachine.SimplifiedStateEvent
import com.r3corda.node.services.statemachine.SimplifiedStateMachineManager
import com.r3corda.protocols.SimplifiedRatesFixProtocol.FixOutOfRange
import java.io.Serializable
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.util.*

// This code is unit tested in NodeInterestRates.kt

/**
 * This protocol queries the given oracle for an interest rate fix, and if it is within the given tolerance embeds the
 * fix in the transaction and then proceeds to get the oracle to sign it. Although the [call] method combines the query
 * and signing step, you can run the steps individually by constructing this object and then using the public methods
 * for each step.
 *
 * @throws FixOutOfRange if the returned fix was further away from the expected rate by the given amount.
 */
open class SimplifiedRatesFixProtocol() : SimplifiedProtocolStateMachine<SimplifiedRatesFixProtocol.Context, SimplifiedRatesFixProtocol.State> {
    data class Context(val tx: TransactionBuilder,
                       val oracle: Party,
                       val fixOf: FixOf,
                       val expectedRate: BigDecimal,
                       val rateTolerance: BigDecimal,
                       val timeOut: Duration,
                       val fix: Fix) : Serializable

    enum class State {
        QUERYING,
        WORKING,
        SIGNING
    }

    companion object {
        val TOPIC = "platform.rates.interest.fix"
    }

    val topic: String get() = TOPIC

    class FixOutOfRange(val byAmount: BigDecimal) : Exception()

    data class QueryRequest(val queries: List<FixOf>, override val replyToParty: Party, override val sessionID: Long, val deadline: Instant) : PartyRequestMessage
    data class SignRequest(val tx: WireTransaction, override val replyToParty: Party, override val sessionID: Long) : PartyRequestMessage

    override val first: State
        get() = State.QUERYING

    override fun getStage(stage: State): (SimplifiedStateMachineManager, SimplifiedStateMachineManager.MachineState<Context, State>, Context, SimplifiedStateEvent) -> Unit
            = when (stage) {
        State.QUERYING -> { smm: SimplifiedStateMachineManager, state: SimplifiedStateMachineManager.MachineState<Context, State>, context: Context, event: SimplifiedStateEvent ->
            query(smm, state, context, event)
        }
        State.SIGNING -> { smm: SimplifiedStateMachineManager, state: SimplifiedStateMachineManager.MachineState<Context, State>, context: Context, event: SimplifiedStateEvent ->
            sign(context, event)
        }
        State.WORKING -> { smm: SimplifiedStateMachineManager, state: SimplifiedStateMachineManager.MachineState<Context, State>, context: Context, event: SimplifiedStateEvent ->
            work(smm, state, context, event)
        }
    }

    private fun query(smm: SimplifiedStateMachineManager,
                      state: SimplifiedStateMachineManager.MachineState<Context, State>,
                      context: SimplifiedRatesFixProtocol.Context,
                      event: SimplifiedStateEvent) {
        require(event is SimplifiedStateEvent.Start)

        val fixOf = context.fixOf
        val serviceHub = smm.serviceHub
        val oracle = context.oracle
        val sessionID = random63BitValue()
        val deadline = suggestInterestRateAnnouncementTimeWindow(fixOf.name, oracle.name, fixOf.forDay).end
        val req = QueryRequest(listOf(fixOf), serviceHub.storageService.myLegalIdentity, sessionID, deadline)
        // TODO: add deadline to receive
        state.sendAndReceive<ArrayList<*>>(smm, topic, oracle, 0, sessionID, req,
                ArrayList::class.java, context, State.WORKING)
    }

    private fun work(smm: SimplifiedStateMachineManager,
                     state: SimplifiedStateMachineManager.MachineState<Context, State>,
                     context: SimplifiedRatesFixProtocol.Context,
                     event: SimplifiedStateEvent) {
        val tx = context.tx
        val oracle = context.oracle
        val fixOf = context.fixOf
        val resp = when (event) {
            is SimplifiedStateEvent.MessageReceived<*> -> event.content as UntrustworthyData<ArrayList<Fix>>
            else -> throw IllegalStateException()
        }
        val fix = resp.validate {
            val fix = it.first()
            // Check the returned fix is for what we asked for.
            check(fix.of == fixOf)
            fix
        }
        checkFixIsNearExpected(context, fix)
        tx.addCommand(fix, oracle.owningKey)
        beforeSigning(smm, context, fix)

        val sessionID = random63BitValue()
        val wtx = tx.toWireTransaction()
        val req = SignRequest(wtx, smm.serviceHub.storageService.myLegalIdentity, sessionID)
        state.sendAndReceive(smm, topic, oracle, 0, sessionID, req,
                DigitalSignature.LegallyIdentifiable::class.java, context,
                SimplifiedRatesFixProtocol.State.SIGNING)
    }

    /**
     * You can override this to perform any additional work needed after the fix is added to the transaction but
     * before it's sent back to the oracle for signing (for example, adding output states that depend on the fix).
     */
    @Suspendable
    @Suppress("UNUSED")
    protected open fun beforeSigning(smm: SimplifiedStateMachineManager,
                                     context: SimplifiedRatesFixProtocol.Context,
                                     fix: Fix) {
    }

    private fun checkFixIsNearExpected(context: Context, fix: Fix) {
        val delta = (fix.value - context.expectedRate).abs()
        if (delta > context.rateTolerance) {
            // TODO: Kick to a user confirmation / ui flow if it's out of bounds instead of raising an exception.
            throw FixOutOfRange(delta)
        }
    }

    private fun sign(context: SimplifiedRatesFixProtocol.Context,
                     event: SimplifiedStateEvent) {
        val tx = context.tx
        val resp = when (event) {
            is SimplifiedStateEvent.MessageReceived<*> -> event.content as UntrustworthyData<DigitalSignature.LegallyIdentifiable>
            else -> throw IllegalStateException()
        }

        val signature = resp.validate { sig ->
            check(sig.signer == context.oracle)
            tx.checkSignature(sig)
            sig
        }
        context.tx.addSignatureUnchecked(signature)
        // TODO: Notify someone we've finished
    }
}
