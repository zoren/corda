package com.r3corda.node.services.statemachine

import com.r3corda.core.crypto.Party
import com.r3corda.core.messaging.Message
import com.r3corda.core.messaging.MessageRecipients
import com.r3corda.core.node.ServiceHub
import com.r3corda.core.serialization.deserialize
import com.r3corda.core.serialization.serialize
import com.r3corda.core.utilities.UntrustworthyData
import java.io.Serializable
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore

/**
 * A much simpler state machine manager which requires that protocols manually manage their
 * own states. Increases transparency at the cost of additional development effort.
 */
// TODO: This needs to store machine states and awaited messages in a persistence layer,
// and only cache them in memory for speed.
class SimplifiedStateMachineManager(val serviceHub: ServiceHub) {
    private val net = serviceHub.networkService
    // TODO: This shouldn't be a fixed pool
    private val executor: Executor = Executors.newFixedThreadPool(3)
    private val machines = HashMap<UUID, MachineState<*, *>>()
    private val semaphores = HashMap<UUID, Semaphore>()

    data class MachineState<C : Serializable, S : Enum<*>>(val id: UUID,
                                                           val machine: SimplifiedProtocolStateMachine<C, S>,
                                                           val awaitingMessages: MutableList<FiberRequest> = Collections.synchronizedList(ArrayList()),
                                                           val workQueue: ConcurrentLinkedQueue<Work<C, S>> = ConcurrentLinkedQueue()) {
        fun call(smm: SimplifiedStateMachineManager) {
            val semaphore = smm.semaphores[id]!!

            // While there is work to do, try to acquire the semaphore. If we successfully acquire the
            // semaphore, we are the thread that's ready to do the work, process it. If we cannot acquire
            // the semaphore, another thread is doing the work.
            //
            // This way, if one thread is processing, and another adds work then tries starting a new
            // executor, either the first thread picks up the work and the second cannot acquire the
            // semaphore, or the first thread releases the semaphore and the second acquires it and
            // picks up the work.
            while (workQueue.isNotEmpty()) {
                if (semaphore.tryAcquire()) {
                    try {
                        doWork(smm)
                    } finally {
                        semaphore.release()
                    }
                } else {
                    return
                }
            }
        }

        private fun doWork(smm: SimplifiedStateMachineManager) {
            var work = workQueue.poll()
            while (work != null) {
                work.call(smm, this, machine)
            }
            work = workQueue.poll()
        }

        fun <T : Any> receive(smm: SimplifiedStateMachineManager,
                              topic: String,
                              sessionIDForReceive: Long,
                              recvType: Class<T>,
                              context: C,
                              next: S)
            = receive(smm, topic, null, -1, sessionIDForReceive, recvType, context, next)

        fun <T : Any> receive(smm: SimplifiedStateMachineManager,
                              topic: String,
                              destination: Party?,
                              sessionIDForSend: Long,
                              sessionIDForReceive: Long,
                              recvType: Class<T>,
                              context: C,
                              next: S)
            = interateOnMessage<T>(smm, FiberRequest.ExpectingResponse<T>(topic, destination, sessionIDForSend,
                    sessionIDForReceive, null, recvType), recvType, context, next)


        fun <T : Any> interateOnMessage(smm: SimplifiedStateMachineManager,
                                        request: FiberRequest,
                                        recvType: Class<T>,
                                        context: C,
                                        next: S) {
            val fullTopic = request.topic + "." + request.sessionIDForReceive
            awaitingMessages.add(request)

            smm.net.addMessageHandler(fullTopic) { msg, reg ->
                val content = UntrustworthyData(msg.data.deserialize<T>())
                workQueue.add(SimplifiedStateMachineManager.Work(context, next, SimplifiedStateEvent.MessageReceived(content)))
                smm.executor.execute { call(smm) }
                smm.net.removeMessageHandler(reg)
            }
        }

        /**
         * @param context the current state of the machine, passed to the next stage when it's called.
         * @param next the next stage to pass control to.
         */
        fun <T : Any> sendAndReceive(smm: SimplifiedStateMachineManager,
                                     topic: String,
                                     destination: Party,
                                     sessionIDForSend: Long,
                                     sessionIDForReceive: Long,
                                     payload: Any,
                                     recvType: Class<T>,
                                     context: C,
                                     next: S) {
            send(smm, topic, destination, sessionIDForSend, payload)
            receive(smm, topic, sessionIDForReceive, recvType, context, next)
        }

        fun send(smm: SimplifiedStateMachineManager,
                 topic: String, destination: Party, sessionID: Long, payload: Any)
                = smm.net.send(smm.net.createMessage(topic + "." + sessionID, payload.serialize().bits),
                smm.serviceHub.networkMapCache.getNodeByPublicKey(destination.owningKey)!!.address)

        fun send(smm: SimplifiedStateMachineManager,
                 topic: String, destination: MessageRecipients, sessionID: Long, payload: Any)
                = smm.net.send(smm.net.createMessage(topic + "." + sessionID, payload.serialize().bits), destination)
    }

    data class Work<C : Serializable, S : Enum<*>>(val context: C, val next: S, val event: SimplifiedStateEvent) {
        fun call(smm: SimplifiedStateMachineManager,
                 state: MachineState<C, S>,
                 machine: SimplifiedProtocolStateMachine<C, S>)
                = machine.getStage(next)(smm, state, context, event)
    }

    fun <C : Serializable, S : Enum<*>> add(machine: SimplifiedProtocolStateMachine<C, S>, context: C) {
        val id = UUID.randomUUID()
        // TODO: This should be in a persistance layer, not just in memory
        val state = MachineState(id, machine)
        machines[id] = state
        semaphores[id] = Semaphore(1)
        state.workQueue.add(Work(context, machine.first, SimplifiedStateEvent.Start()))
        executor.execute { state.call(this) }
    }
}