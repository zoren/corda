package net.corda.core.serialization

import net.corda.core.serialization.Singletons.DEFAULT_SERIALIZATION_FACTORY
import net.corda.core.serialization.Singletons.P2P_CONTEXT
import net.corda.core.utilities.ByteSequence
import net.corda.core.utilities.opaque
import java.io.NotSerializableException
import kotlin.reflect.KProperty

interface SerializationScheme {
    // byteSequence expected to just be the 8 bytes necessary for versioning
    fun canDeserializeVersion(byteSequence: ByteSequence, target: SerializationContext.Target): Boolean

    @Throws(NotSerializableException::class)
    fun <T : Any> deserialize(byteSequence: ByteSequence, clazz: Class<T>, context: SerializationContext): T

    @Throws(NotSerializableException::class)
    fun <T : Any> serialize(obj: T, context: SerializationContext): SerializedBytes<T>
}

interface SerializationFactory {
    val currentContext: SerializationContext

    @Throws(NotSerializableException::class)
    fun <T : Any> deserialize(byteSequence: ByteSequence, clazz: Class<T>, context: SerializationContext = currentContext): T

    @Throws(NotSerializableException::class)
    fun <T : Any> serialize(obj: T, context: SerializationContext = currentContext): SerializedBytes<T>
}

interface SerializationContext {
    val preferedSerializationVersion: ByteSequence
    val deserializationClassLoader: ClassLoader
    val whitelist: ClassWhitelist
    val properties: Map<Any, Any>
    val objectReferencesEnabled: Boolean
    val target: Target

    fun withProperty(property: Any, value: Any): SerializationContext
    fun withoutReferences(): SerializationContext
    fun withClassLoader(classLoader: ClassLoader): SerializationContext
    fun withWhitelisted(clazz: Class<*>): SerializationContext

    enum class Target { P2P, RPCServer, RPCClient, Storage, Quasar }
}

class WriteOnceProperty<T : Any>() {
    private var v: T? = null

    operator fun getValue(thisRef: Any?, property: KProperty<*>) = v ?: throw NullPointerException("Write-once property $property not set.")

    operator fun setValue(thisRef: Any?, property: KProperty<*>, value: T) {
        if (v != null) throw IllegalStateException("Cannot set write-once property $property more than once.")
        v = value
    }
}

object Singletons {
    var DEFAULT_SERIALIZATION_FACTORY: SerializationFactory by WriteOnceProperty()
    var P2P_CONTEXT: SerializationContext by WriteOnceProperty()
    var RPC_SERVER_CONTEXT: SerializationContext by WriteOnceProperty()
    var RPC_CLIENT_CONTEXT: SerializationContext by WriteOnceProperty()
    var STORAGE_CONTEXT: SerializationContext by WriteOnceProperty()
    var CHECKPOINT_CONTEXT: SerializationContext by WriteOnceProperty()
}

inline fun <reified T : Any> ByteSequence.deserialize(serializationFactory: SerializationFactory = DEFAULT_SERIALIZATION_FACTORY, context: SerializationContext = P2P_CONTEXT): T {
    return serializationFactory.deserialize(this, T::class.java, context)
}

inline fun <reified T : Any> SerializedBytes<T>.deserialize(serializationFactory: SerializationFactory = DEFAULT_SERIALIZATION_FACTORY, context: SerializationContext = P2P_CONTEXT): T {
    return serializationFactory.deserialize(this, T::class.java, context)
}

fun <T : Any> T.serialize(serializationFactory: SerializationFactory = DEFAULT_SERIALIZATION_FACTORY, context: SerializationContext = P2P_CONTEXT): SerializedBytes<T> {
    return serializationFactory.serialize(this, context)
}

inline fun <reified T : Any> ByteArray.deserialize(serializationFactory: SerializationFactory = DEFAULT_SERIALIZATION_FACTORY, context: SerializationContext = P2P_CONTEXT): T = this.opaque().deserialize(serializationFactory, context)
