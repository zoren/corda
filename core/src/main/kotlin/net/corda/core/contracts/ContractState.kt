package net.corda.core.contracts

import net.corda.core.identity.AbstractParty
import net.corda.core.serialization.CordaSerializable

typealias ContractClassName = String

/**
 * A contract state (or just "state") contains opaque data used by a contract program. It can be thought of as a disk
 * file that the program can use to persist data across transactions. States are immutable: once created they are never
 * updated, instead, any changes must generate a new successor state. States can be updated (consumed) only once: the
 * notary is responsible for ensuring there is no "double spending" by only signing a transaction if the input states
 * are all free.
 */
@CordaSerializable
interface ContractState {
    /**
     * An instance of the contract class that will verify this state.
     *
     * # Discussion
     *
     * This field is not the final design, it's just a piece of temporary scaffolding. Once the contract sandbox is
     * further along, this field will become a description of which attachments are acceptable for defining the
     * contract.
     *
     * Recall that an attachment is a zip file that can be referenced from any transaction. The contents of the
     * attachments are merged together and cannot define any overlapping files, thus for any given transaction there
     * is a miniature file system in which each file can be precisely mapped to the defining attachment.
     *
     * Attachments may contain many things (data files, legal documents, etc) but mostly they contain JVM bytecode.
     * The class files inside define not only [Contract] implementations but also the classes that define the states.
     * Within the rest of a transaction, user-providable components are referenced by name only.
     *
     * This means that a smart contract in Corda does two things:
     *
     * 1. Define the data structures that compose the ledger (the states)
     * 2. Define the rules for updating those structures
     *
     * The first is merely a utility role ... in theory contract code could manually parse byte streams by hand.
     * The second is vital to the integrity of the ledger. So this field needs to be able to express constraints like:
     *
     * - Only attachment 733c350f396a727655be1363c06635ba355036bd54a5ed6e594fd0b5d05f42f6 may be used with this state.
     * - Any attachment signed by public key 2d1ce0e330c52b8055258d776c40 may be used with this state.
     * - Attachments (1, 2, 3) may all be used with this state.
     *
     * and so on. In this way it becomes possible for the business logic governing a state to be evolved, if the
     * constraints are flexible enough.
     *
     * Because contract classes often also define utilities that generate relevant transactions, and because attachments
     * cannot know their own hashes, we will have to provide various utilities to assist with obtaining the right
     * code constraints from within the contract code itself.
     *
     * TODO: Implement the above description. See COR-226
     */
    val contract: ContractClassName

    /**
     * A _participant_ is any party that is able to consume this state in a valid transaction.
     *
     * The list of participants is required for certain types of transactions. For example, when changing the notary
     * for this state, every participant has to be involved and approve the transaction
     * so that they receive the updated state, and don't end up in a situation where they can no longer use a state
     * they possess, since someone consumed that state during the notary change process.
     *
     * The participants list should normally be derived from the contents of the state.
     */
    val participants: List<AbstractParty>
}