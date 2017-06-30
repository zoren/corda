package net.corda.client.rpc.serialization

import com.esotericsoftware.kryo.pool.KryoPool
import net.corda.client.rpc.internal.RpcClientObservableSerializer
import net.corda.core.serialization.DefaultKryoCustomizer
import net.corda.core.serialization.KryoHeaderV0_1
import net.corda.core.serialization.SerializationContext
import net.corda.core.utilities.ByteSequence
import net.corda.nodeapi.RPCKryo
import net.corda.nodeapi.serialization.AbstractKryoSerializationScheme


class KryoClientSerializationScheme : AbstractKryoSerializationScheme() {
    override fun canDeserializeVersion(byteSequence: ByteSequence, target: SerializationContext.Target): Boolean {
        return byteSequence.equals(KryoHeaderV0_1) && (target == SerializationContext.Target.RPCClient || target == SerializationContext.Target.P2P)
    }

    override fun rpcClientKryoPool(context: SerializationContext): KryoPool {
        return KryoPool.Builder {
            DefaultKryoCustomizer.customize(RPCKryo(RpcClientObservableSerializer, context.whitelist)).apply { classLoader = context.deserializationClassLoader }
        }.build()
    }

    override fun rpcServerKryoPool(context: SerializationContext): KryoPool {
        throw UnsupportedOperationException()
    }
}