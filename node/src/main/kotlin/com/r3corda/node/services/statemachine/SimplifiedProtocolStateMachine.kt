package com.r3corda.node.services.statemachine

import java.io.Serializable


/**
 * The interface of [ProtocolStateMachineImpl] exposing methods and properties required by ProtocolLogic
 * for compilation.
 *
 * @param C the type of context object used by the state machine.
 * @param S the enumerated set of stages this machine can be in.
 */
interface SimplifiedProtocolStateMachine<C : Serializable, S: Enum<*>> {
    val first: S
    fun getStage(stage: S)
            : (SimplifiedStateMachineManager, SimplifiedStateMachineManager.MachineState<C, S>, C, SimplifiedStateEvent) -> Unit
}
