package net.corda.nodeapi.internal.serialization

import com.esotericsoftware.kryo.Kryo
import net.corda.core.serialization.SerializationCustomization

class KryoSerializationCustomization(val kryo: Kryo) : SerializationCustomization {
    override fun addToWhitelist(vararg types: Class<*>) {
        kryo.addToWhitelist(*types)
    }
}