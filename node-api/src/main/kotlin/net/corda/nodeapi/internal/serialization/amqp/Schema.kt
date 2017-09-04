package net.corda.nodeapi.internal.serialization.amqp

import com.google.common.hash.Hasher
import com.google.common.hash.Hashing
import net.corda.core.crypto.toBase64
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.loggerFor
import org.apache.qpid.proton.amqp.DescribedType
import org.apache.qpid.proton.amqp.UnsignedInteger
import org.apache.qpid.proton.amqp.UnsignedLong
import org.apache.qpid.proton.codec.Data
import org.apache.qpid.proton.codec.DescribedTypeConstructor
import java.io.NotSerializableException
import java.lang.reflect.*
import java.util.*

import net.corda.nodeapi.internal.serialization.carpenter.Field as CarpenterField
import net.corda.nodeapi.internal.serialization.carpenter.Schema as CarpenterSchema

// TODO: get an assigned number as per AMQP spec
val DESCRIPTOR_TOP_32BITS: Long = 0xc0da0000

val DESCRIPTOR_DOMAIN: String = "net.corda"

// "corda" + majorVersionByte + minorVersionMSB + minorVersionLSB
val AmqpHeaderV1_0: OpaqueBytes = OpaqueBytes("corda\u0001\u0000\u0000".toByteArray())

private enum class DescriptorRegistry(val id: Long) {

    ENVELOPE(1),
    SCHEMA(2),
    OBJECT_DESCRIPTOR(3),
    FIELD(4),
    COMPOSITE_TYPE(5),
    RESTRICTED_TYPE(6),
    CHOICE(7),
    REFERENCED_OBJECT(8),
    ;

    val amqpDescriptor = UnsignedLong(id or DESCRIPTOR_TOP_32BITS)
}

/**
 * This class wraps all serialized data, so that the schema can be carried along with it.  We will provide various internal utilities
 * to decompose and recompose with/without schema etc so that e.g. we can store objects with a (relationally) normalised out schema to
 * avoid excessive duplication.
 */
// TODO: make the schema parsing lazy since mostly schemas will have been seen before and we only need it if we don't recognise a type descriptor.
data class Envelope(val obj: Any?, val schema: Schema) : DescribedType {
    companion object : DescribedTypeConstructor<Envelope> {
        val DESCRIPTOR = DescriptorRegistry.ENVELOPE.amqpDescriptor
        val DESCRIPTOR_OBJECT = Descriptor(null, DESCRIPTOR)

        fun get(data: Data): Envelope {
            val describedType = data.`object` as DescribedType
            if (describedType.descriptor != DESCRIPTOR) {
                throw NotSerializableException("Unexpected descriptor ${describedType.descriptor}.")
            }
            val list = describedType.described as List<*>
            return newInstance(listOf(list[0], Schema.get(list[1]!!)))
        }

        override fun getTypeClass(): Class<*> = Envelope::class.java

        override fun newInstance(described: Any?): Envelope {
            val list = described as? List<*> ?: throw IllegalStateException("Was expecting a list")
            return Envelope(list[0], list[1] as Schema)
        }
    }

    override fun getDescriptor(): Any = DESCRIPTOR

    override fun getDescribed(): Any = listOf(obj, schema)
}

/**
 * This and the classes below are OO representations of the AMQP XML schema described in the specification. Their
 * [toString] representations generate the associated XML form.
 */
data class Schema(val types: List<TypeNotation>) : DescribedType {
    companion object : DescribedTypeConstructor<Schema> {
        val DESCRIPTOR = DescriptorRegistry.SCHEMA.amqpDescriptor

        fun get(obj: Any): Schema {
            val describedType = obj as DescribedType
            if (describedType.descriptor != DESCRIPTOR) {
                throw NotSerializableException("Unexpected descriptor ${describedType.descriptor}.")
            }
            val list = describedType.described as List<*>
            return newInstance(listOf((list[0] as List<*>).map { TypeNotation.get(it!!) }))
        }

        override fun getTypeClass(): Class<*> = Schema::class.java

        override fun newInstance(described: Any?): Schema {
            val list = described as? List<*> ?: throw IllegalStateException("Was expecting a list")
            @Suppress("UNCHECKED_CAST")
            return Schema(list[0] as List<TypeNotation>)
        }
    }

    override fun getDescriptor(): Any = DESCRIPTOR

    override fun getDescribed(): Any = listOf(types)

    override fun toString(): String = types.joinToString("\n")
}

data class Descriptor(val name: String?, val code: UnsignedLong? = null) : DescribedType {
    companion object : DescribedTypeConstructor<Descriptor> {
        val DESCRIPTOR = DescriptorRegistry.OBJECT_DESCRIPTOR.amqpDescriptor

        fun get(obj: Any): Descriptor {
            val describedType = obj as DescribedType
            if (describedType.descriptor != DESCRIPTOR) {
                throw NotSerializableException("Unexpected descriptor ${describedType.descriptor}.")
            }
            return newInstance(describedType.described)
        }

        override fun getTypeClass(): Class<*> = Descriptor::class.java

        override fun newInstance(described: Any?): Descriptor {
            val list = described as? List<*> ?: throw IllegalStateException("Was expecting a list")
            return Descriptor(list[0] as? String, list[1] as? UnsignedLong)
        }
    }

    override fun getDescriptor(): Any = DESCRIPTOR

    override fun getDescribed(): Any = listOf(name, code)

    override fun toString(): String {
        val sb = StringBuilder("<descriptor")
        if (name != null) {
            sb.append(" name=\"$name\"")
        }
        if (code != null) {
            val code = String.format("0x%08x:0x%08x", code.toLong().shr(32), code.toLong().and(0xffff))
            sb.append(" code=\"$code\"")
        }
        sb.append("/>")
        return sb.toString()
    }
}

data class Field(val name: String, val type: String, val requires: List<String>, val default: String?, val label: String?, val mandatory: Boolean, val multiple: Boolean) : DescribedType {
    companion object : DescribedTypeConstructor<Field> {
        val DESCRIPTOR = DescriptorRegistry.FIELD.amqpDescriptor

        fun get(obj: Any): Field {
            val describedType = obj as DescribedType
            if (describedType.descriptor != DESCRIPTOR) {
                throw NotSerializableException("Unexpected descriptor ${describedType.descriptor}.")
            }
            return newInstance(describedType.described)
        }

        override fun getTypeClass(): Class<*> = Field::class.java

        override fun newInstance(described: Any?): Field {
            val list = described as? List<*> ?: throw IllegalStateException("Was expecting a list")
            @Suppress("UNCHECKED_CAST")
            return Field(list[0] as String, list[1] as String, list[2] as List<String>, list[3] as? String, list[4] as? String, list[5] as Boolean, list[6] as Boolean)
        }
    }

    override fun getDescriptor(): Any = DESCRIPTOR

    override fun getDescribed(): Any = listOf(name, type, requires, default, label, mandatory, multiple)

    override fun toString(): String {
        val sb = StringBuilder("<field name=\"$name\" type=\"$type\" mandatory=\"$mandatory\" multiple=\"$multiple\"")
        if (requires.isNotEmpty()) {
            sb.append(" requires=\"")
            sb.append(requires.joinToString(","))
            sb.append("\"")
        }
        if (default != null) {
            sb.append(" default=\"$default\"")
        }
        if (!label.isNullOrBlank()) {
            sb.append(" label=\"$label\"")
        }
        sb.append("/>")
        return sb.toString()
    }
}

sealed class TypeNotation : DescribedType {
    companion object {
        fun get(obj: Any): TypeNotation {
            val describedType = obj as DescribedType
            return when (describedType.descriptor) {
                CompositeType.DESCRIPTOR -> CompositeType.get(describedType)
                RestrictedType.DESCRIPTOR -> RestrictedType.get(describedType)
                else -> throw NotSerializableException("Unexpected descriptor ${describedType.descriptor}.")
            }
        }
    }

    abstract val name: String
    abstract val label: String?
    abstract val provides: List<String>
    abstract val descriptor: Descriptor
}

data class CompositeType(override val name: String, override val label: String?, override val provides: List<String>, override val descriptor: Descriptor, val fields: List<Field>) : TypeNotation() {
    companion object : DescribedTypeConstructor<CompositeType> {
        val DESCRIPTOR = DescriptorRegistry.COMPOSITE_TYPE.amqpDescriptor

        fun get(describedType: DescribedType): CompositeType {
            if (describedType.descriptor != DESCRIPTOR) {
                throw NotSerializableException("Unexpected descriptor ${describedType.descriptor}.")
            }
            val list = describedType.described as List<*>
            return newInstance(listOf(list[0], list[1], list[2], Descriptor.get(list[3]!!), (list[4] as List<*>).map { Field.get(it!!) }))
        }

        override fun getTypeClass(): Class<*> = CompositeType::class.java

        override fun newInstance(described: Any?): CompositeType {
            val list = described as? List<*> ?: throw IllegalStateException("Was expecting a list")
            @Suppress("UNCHECKED_CAST")
            return CompositeType(list[0] as String, list[1] as? String, list[2] as List<String>, list[3] as Descriptor, list[4] as List<Field>)
        }
    }

    override fun getDescriptor(): Any = DESCRIPTOR

    override fun getDescribed(): Any = listOf(name, label, provides, descriptor, fields)

    override fun toString(): String {
        val sb = StringBuilder("<type class=\"composite\" name=\"$name\"")
        if (!label.isNullOrBlank()) {
            sb.append(" label=\"$label\"")
        }
        if (provides.isNotEmpty()) {
            sb.append(" provides=\"")
            sb.append(provides.joinToString(","))
            sb.append("\"")
        }
        sb.append(">\n")
        sb.append("  $descriptor\n")
        for (field in fields) {
            sb.append("  $field\n")
        }
        sb.append("</type>")
        return sb.toString()
    }
}

data class RestrictedType(override val name: String,
                          override val label: String?,
                          override val provides: List<String>,
                          val source: String,
                          override val descriptor: Descriptor,
                          val choices: List<Choice>) : TypeNotation() {
    companion object : DescribedTypeConstructor<RestrictedType> {
        val DESCRIPTOR = DescriptorRegistry.RESTRICTED_TYPE.amqpDescriptor

        fun get(describedType: DescribedType): RestrictedType {
            if (describedType.descriptor != DESCRIPTOR) {
                throw NotSerializableException("Unexpected descriptor ${describedType.descriptor}.")
            }
            val list = describedType.described as List<*>
            return newInstance(listOf(list[0], list[1], list[2], list[3], Descriptor.get(list[4]!!), (list[5] as List<*>).map { Choice.get(it!!) }))
        }

        override fun getTypeClass(): Class<*> = RestrictedType::class.java

        override fun newInstance(described: Any?): RestrictedType {
            val list = described as? List<*> ?: throw IllegalStateException("Was expecting a list")
            @Suppress("UNCHECKED_CAST")
            return RestrictedType(list[0] as String, list[1] as? String, list[2] as List<String>, list[3] as String, list[4] as Descriptor, list[5] as List<Choice>)
        }
    }

    override fun getDescriptor(): Any = DESCRIPTOR

    override fun getDescribed(): Any = listOf(name, label, provides, source, descriptor, choices)

    override fun toString(): String {
        val sb = StringBuilder("<type class=\"restricted\" name=\"$name\"")
        if (!label.isNullOrBlank()) {
            sb.append(" label=\"$label\"")
        }
        sb.append(" source=\"$source\"")
        if (provides.isNotEmpty()) {
            sb.append(" provides=\"")
            sb.append(provides.joinToString(","))
            sb.append("\"")
        }
        sb.append(">\n")
        sb.append("  $descriptor\n")
        choices.forEach {
            sb.append("  $it\n")
        }
        sb.append("</type>")
        return sb.toString()
    }
}

data class Choice(val name: String, val value: String) : DescribedType {
    companion object : DescribedTypeConstructor<Choice> {
        val DESCRIPTOR = DescriptorRegistry.CHOICE.amqpDescriptor

        fun get(obj: Any): Choice {
            val describedType = obj as DescribedType
            if (describedType.descriptor != DESCRIPTOR) {
                throw NotSerializableException("Unexpected descriptor ${describedType.descriptor}.")
            }
            return newInstance(describedType.described)
        }

        override fun getTypeClass(): Class<*> = Choice::class.java

        override fun newInstance(described: Any?): Choice {
            val list = described as? List<*> ?: throw IllegalStateException("Was expecting a list")
            return Choice(list[0] as String, list[1] as String)
        }
    }

    override fun getDescriptor(): Any = DESCRIPTOR

    override fun getDescribed(): Any = listOf(name, value)

    override fun toString(): String {
        return "<choice name=\"$name\" value=\"$value\"/>"
    }
}

data class ReferencedObject(private val refCounter: Int) : DescribedType {
    companion object : DescribedTypeConstructor<ReferencedObject> {
        val DESCRIPTOR = DescriptorRegistry.REFERENCED_OBJECT.amqpDescriptor

        fun get(obj: Any): ReferencedObject {
            val describedType = obj as DescribedType
            if (describedType.descriptor != DESCRIPTOR) {
                throw NotSerializableException("Unexpected descriptor ${describedType.descriptor}.")
            }
            return newInstance(describedType.described)
        }

        override fun getTypeClass(): Class<*> = ReferencedObject::class.java

        override fun newInstance(described: Any?): ReferencedObject {
            val unInt = described as? UnsignedInteger ?: throw IllegalStateException("Was expecting an UnsignedInteger")
            return ReferencedObject(unInt.toInt())
        }
    }

    override fun getDescriptor(): Any = DESCRIPTOR

    override fun getDescribed(): UnsignedInteger = UnsignedInteger(refCounter)

    override fun toString(): String = "<refObject refCounter=$refCounter/>"
}

private val ARRAY_HASH: String = "Array = true"
private val ENUM_HASH: String = "Enum = true"
private val ALREADY_SEEN_HASH: String = "Already seen = true"
private val NULLABLE_HASH: String = "Nullable = true"
private val NOT_NULLABLE_HASH: String = "Nullable = false"
private val ANY_TYPE_HASH: String = "Any type = true"
private val TYPE_VARIABLE_HASH: String = "Type variable = true"
private val WILDCARD_TYPE_HASH: String = "Wild card = true"

private val logger by lazy { loggerFor<Schema>() }

/**
 * The method generates a fingerprint for a given JVM [Type] that should be unique to the schema representation.
 * Thus it only takes into account properties and types and only supports the same object graph subset as the overall
 * serialization code.
 *
 * The idea being that even for two classes that share the same name but differ in a minor way, the fingerprint will be
 * different.
 */
// TODO: write tests
internal fun fingerprintForType(type: Type, factory: SerializerFactory): String {
    return fingerprintForType(type, null, HashSet(), Hashing.murmur3_128().newHasher(), factory).hash().asBytes().toBase64()
}

internal fun fingerprintForDescriptors(vararg typeDescriptors: String): String {
    val hasher = Hashing.murmur3_128().newHasher()
    for (typeDescriptor in typeDescriptors) {
        hasher.putUnencodedChars(typeDescriptor)
    }
    return hasher.hash().asBytes().toBase64()
}

private fun Hasher.fingerprintWithCustomSerializerOrElse(factory: SerializerFactory, clazz: Class<*>, declaredType: Type, block: () -> Hasher) : Hasher {
    // Need to check if a custom serializer is applicable
    val customSerializer = factory.findCustomSerializer(clazz, declaredType)
    return if (customSerializer != null) {
        putUnencodedChars(customSerializer.typeDescriptor)
    } else {
        block()
    }
}

// This method concatentates various elements of the types recursively as unencoded strings into the hasher, effectively
// creating a unique string for a type which we then hash in the calling function above.
private fun fingerprintForType(type: Type, contextType: Type?, alreadySeen: MutableSet<Type>, hasher: Hasher, factory: SerializerFactory): Hasher {
    return if (type in alreadySeen) {
        hasher.putUnencodedChars(ALREADY_SEEN_HASH)
    } else {
        alreadySeen += type
        try {
            when (type) {
                is SerializerFactory.AnyType -> hasher.putUnencodedChars(ANY_TYPE_HASH)
                is Class<*> -> when {
                    type.isArray -> fingerprintForType(type.componentType, contextType, alreadySeen, hasher, factory).putUnencodedChars(ARRAY_HASH)
                    SerializerFactory.isPrimitive(type) ||
                    isCollectionOrMap(type) -> hasher.putUnencodedChars(type.name)
                    type.isEnum -> {
                        // ensures any change to the enum (adding constants) will trigger the need for evolution
                        hasher.apply { type.enumConstants.forEach { putUnencodedChars(it.toString())
                        }}.putUnencodedChars(type.name).putUnencodedChars(ENUM_HASH)
                    }
                    else ->
                        hasher.fingerprintWithCustomSerializerOrElse(factory, type, type) {
                        if (type.kotlin.objectInstance != null) {
                            // TODO: name collision is too likely for kotlin objects, we need to introduce some reference
                            // to the CorDapp but maybe reference to the JAR in the short term.
                            hasher.putUnencodedChars(type.name)
                        } else {
                            fingerprintForObject(type, type, alreadySeen, hasher, factory)
                        }
                    }
                }
                is ParameterizedType -> {
                    // Hash the rawType + params
                    val clazz = type.rawType as Class<*>
                    val startingHash = if (isCollectionOrMap(clazz)) {
                        hasher.putUnencodedChars(clazz.name)
                    } else {
                        hasher.fingerprintWithCustomSerializerOrElse(factory, clazz, type) {
                            fingerprintForObject(type, type, alreadySeen, hasher, factory)
                        }
                    }
                    // ... and concatentate the type data for each parameter type.
                    type.actualTypeArguments.fold(startingHash) {
                        orig, paramType -> fingerprintForType(paramType, type, alreadySeen, orig, factory)
                    }
                }
                // Hash the element type + some array hash
                is GenericArrayType -> fingerprintForType(type.genericComponentType, contextType, alreadySeen,
                        hasher, factory).putUnencodedChars(ARRAY_HASH)
                // TODO: include bounds
                is TypeVariable<*> -> hasher.putUnencodedChars(type.name).putUnencodedChars(TYPE_VARIABLE_HASH)
                is WildcardType -> hasher.putUnencodedChars(type.typeName).putUnencodedChars(WILDCARD_TYPE_HASH)
                else -> throw NotSerializableException("Don't know how to hash")
            }
        } catch(e: NotSerializableException) {
            val msg = "${e.message} -> $type"
            logger.error(msg, e)
            throw NotSerializableException(msg)
        }
    }
}

private fun isCollectionOrMap(type: Class<*>) = Collection::class.java.isAssignableFrom(type) || Map::class.java.isAssignableFrom(type)

private fun fingerprintForObject(type: Type, contextType: Type?, alreadySeen: MutableSet<Type>, hasher: Hasher, factory: SerializerFactory): Hasher {
    // Hash the class + properties + interfaces
    val name = type.asClass()?.name ?: throw NotSerializableException("Expected only Class or ParameterizedType but found $type")
    propertiesForSerialization(constructorForDeserialization(type), contextType ?: type, factory).fold(hasher.putUnencodedChars(name)) { orig, prop ->
        fingerprintForType(prop.resolvedType, type, alreadySeen, orig, factory).putUnencodedChars(prop.name).putUnencodedChars(if (prop.mandatory) NOT_NULLABLE_HASH else NULLABLE_HASH)
    }
    interfacesForSerialization(type, factory).map { fingerprintForType(it, type, alreadySeen, hasher, factory) }
    return hasher
}
