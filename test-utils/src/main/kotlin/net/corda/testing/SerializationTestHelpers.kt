package net.corda.testing

import net.corda.client.rpc.serialization.KryoClientSerializationScheme
import net.corda.core.serialization.*
import net.corda.core.utilities.ByteSequence
import net.corda.node.serialization.KryoServerSerializationScheme
import net.corda.nodeapi.serialization.*

fun <T> withTestSerialization(block: () -> T): T {
    initialiseTestSerialization()
    try {
        return block()
    } finally {
        resetTestSerialization()
    }
}

fun initialiseTestSerialization() {
    // Check that everything is configured for testing with mutable delegating instances.
    try {
        check(Singletons.DEFAULT_SERIALIZATION_FACTORY is TestSerializationFactory) {
            "Found non-test serialization configuration: ${Singletons.DEFAULT_SERIALIZATION_FACTORY}"
        }
    } catch(e: NullPointerException) {
        Singletons.DEFAULT_SERIALIZATION_FACTORY = TestSerializationFactory()
    }
    try {
        check(Singletons.P2P_CONTEXT is TestSerializationContext)
    } catch(e: NullPointerException) {
        Singletons.P2P_CONTEXT = TestSerializationContext()
    }
    try {
        check(Singletons.RPC_SERVER_CONTEXT is TestSerializationContext)
    } catch(e: NullPointerException) {
        Singletons.RPC_SERVER_CONTEXT = TestSerializationContext()
    }
    try {
        check(Singletons.RPC_CLIENT_CONTEXT is TestSerializationContext)
    } catch(e: NullPointerException) {
        Singletons.RPC_CLIENT_CONTEXT = TestSerializationContext()
    }
    try {
        check(Singletons.STORAGE_CONTEXT is TestSerializationContext)
    } catch(e: NullPointerException) {
        Singletons.STORAGE_CONTEXT = TestSerializationContext()
    }
    try {
        check(Singletons.CHECKPOINT_CONTEXT is TestSerializationContext)
    } catch(e: NullPointerException) {
        Singletons.CHECKPOINT_CONTEXT = TestSerializationContext()
    }

    // Check that the previous test, if there was one, cleaned up after itself.
    // IF YOU SEE THESE MESSAGES, THEN IT MEANS A TEST HAS NOT CALLED resetTestSerialization()
    check((Singletons.DEFAULT_SERIALIZATION_FACTORY as TestSerializationFactory).delegate == null, { "Expected uninitialised serialization framework but found it set from: ${Singletons.DEFAULT_SERIALIZATION_FACTORY}" })
    check((Singletons.P2P_CONTEXT as TestSerializationContext).delegate == null, { "Expected uninitialised serialization framework but found it set from: ${Singletons.P2P_CONTEXT}" })
    check((Singletons.RPC_SERVER_CONTEXT as TestSerializationContext).delegate == null, { "Expected uninitialised serialization framework but found it set from: ${Singletons.RPC_SERVER_CONTEXT}" })
    check((Singletons.RPC_CLIENT_CONTEXT as TestSerializationContext).delegate == null, { "Expected uninitialised serialization framework but found it set from: ${Singletons.RPC_CLIENT_CONTEXT}" })
    check((Singletons.STORAGE_CONTEXT as TestSerializationContext).delegate == null, { "Expected uninitialised serialization framework but found it set from: ${Singletons.STORAGE_CONTEXT}" })
    check((Singletons.CHECKPOINT_CONTEXT as TestSerializationContext).delegate == null, { "Expected uninitialised serialization framework but found it set from: ${Singletons.CHECKPOINT_CONTEXT}" })

    // Now configure all the testing related delegates.
    (Singletons.DEFAULT_SERIALIZATION_FACTORY as TestSerializationFactory).delegate = SerializationFactoryImpl().apply {
        registerScheme(KryoClientSerializationScheme())
        registerScheme(KryoServerSerializationScheme())
    }
    (Singletons.P2P_CONTEXT as TestSerializationContext).delegate = KRYO_P2P_CONTEXT
    (Singletons.RPC_SERVER_CONTEXT as TestSerializationContext).delegate = KRYO_RPC_SERVER_CONTEXT
    (Singletons.RPC_CLIENT_CONTEXT as TestSerializationContext).delegate = KRYO_RPC_CLIENT_CONTEXT
    (Singletons.STORAGE_CONTEXT as TestSerializationContext).delegate = KRYO_STORAGE_CONTEXT
    (Singletons.CHECKPOINT_CONTEXT as TestSerializationContext).delegate = KRYO_CHECKPOINT_CONTEXT
//
//    println("SET")
}

fun resetTestSerialization() {
    (Singletons.DEFAULT_SERIALIZATION_FACTORY as TestSerializationFactory).delegate = null
    (Singletons.P2P_CONTEXT as TestSerializationContext).delegate = null
    (Singletons.RPC_SERVER_CONTEXT as TestSerializationContext).delegate = null
    (Singletons.RPC_CLIENT_CONTEXT as TestSerializationContext).delegate = null
    (Singletons.STORAGE_CONTEXT as TestSerializationContext).delegate = null
    (Singletons.CHECKPOINT_CONTEXT as TestSerializationContext).delegate = null
//    println("UNSET")
}

class TestSerializationFactory : SerializationFactory {
    var delegate: SerializationFactory? = null
        set(value) {
            field = value
            stackTrace = Exception().stackTrace.asList()
        }
    private var stackTrace: List<StackTraceElement>? = null

    override fun toString(): String = stackTrace?.joinToString("\n") ?: "null"

    override val currentContext: SerializationContext
        get() = delegate!!.currentContext

    override fun <T : Any> deserialize(byteSequence: ByteSequence, clazz: Class<T>, context: SerializationContext): T {
        return delegate!!.deserialize(byteSequence, clazz, context)
    }

    override fun <T : Any> serialize(obj: T, context: SerializationContext): SerializedBytes<T> {
        return delegate!!.serialize(obj, context)
    }
}

class TestSerializationContext : SerializationContext {
    var delegate: SerializationContext? = null
        set(value) {
            field = value
            stackTrace = Exception().stackTrace.asList()
        }
    private var stackTrace: List<StackTraceElement>? = null

    override fun toString(): String = stackTrace?.joinToString("\n") ?: "null"

    override val preferedSerializationVersion: ByteSequence
        get() = delegate!!.preferedSerializationVersion
    override val deserializationClassLoader: ClassLoader
        get() = delegate!!.deserializationClassLoader
    override val whitelist: ClassWhitelist
        get() = delegate!!.whitelist
    override val properties: Map<Any, Any>
        get() = delegate!!.properties
    override val objectReferencesEnabled: Boolean
        get() = delegate!!.objectReferencesEnabled
    override val target: SerializationContext.Target
        get() = delegate!!.target

    override fun withProperty(property: Any, value: Any): SerializationContext {
        return TestSerializationContext().apply { delegate = this@TestSerializationContext.delegate!!.withProperty(property, value) }
    }

    override fun withoutReferences(): SerializationContext {
        return TestSerializationContext().apply { delegate = this@TestSerializationContext.delegate!!.withoutReferences() }
    }

    override fun withClassLoader(classLoader: ClassLoader): SerializationContext {
        return TestSerializationContext().apply { delegate = this@TestSerializationContext.delegate!!.withClassLoader(classLoader) }
    }

    override fun withWhitelisted(clazz: Class<*>): SerializationContext {
        return TestSerializationContext().apply { delegate = this@TestSerializationContext.delegate!!.withWhitelisted(clazz) }
    }
}
