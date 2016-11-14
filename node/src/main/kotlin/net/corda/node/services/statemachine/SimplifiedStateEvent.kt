package net.corda.node.services.statemachine

import net.corda.core.utilities.UntrustworthyData

/**
 * Events which can occur to trigger a state machine stage.
 */
sealed class SimplifiedStateEvent {
    class Start : SimplifiedStateEvent()
    class MessageReceived<T : Any>(val content: UntrustworthyData<T>) : SimplifiedStateEvent()
    class MachineFinished<M: SimplifiedProtocolStateMachine<*, *>>(val machine: M) : SimplifiedStateEvent()
    class Timeout: SimplifiedStateEvent()
}