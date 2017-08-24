package net.corda.nodeapi.internal.serialization.amqp

import org.apache.qpid.proton.codec.Data
import java.lang.reflect.Type
import java.io.NotSerializableException

class EnumSerializer(declaredType: Type, declaredClass: Class<*>,factory: SerializerFactory) : AMQPSerializer<Any> {
    override val type: Type = declaredType
    override val typeDescriptor = "$DESCRIPTOR_DOMAIN:${fingerprintForType(type, factory)}"
    private val typeNotation: TypeNotation

    init {

        typeNotation = RestrictedType(
                SerializerFactory.nameForType(declaredType),
                null, emptyList(), "enum", Descriptor(typeDescriptor, null),
                declaredClass.enumConstants.zip(IntRange(0, declaredClass.enumConstants.size)).map {
                    Choice (it.first.toString(), it.second.toString())
                })
    }

    override fun writeClassInfo(output: SerializationOutput) {
        output.writeTypeNotations(typeNotation)
    }

    override fun readObject(obj: Any, schema: Schema, input: DeserializationInput): Any {
        val a = input.readObjectOrNull((obj as List<*>)[0], schema, type)
        val b = input.readObjectOrNull(obj[1], schema, type)
        val fromOrd = type.asClass()!!.enumConstants.find { (it as Enum<*>).ordinal == b as Int }

        if (a != fromOrd?.toString()){
            throw NotSerializableException("Deerializing obj as enum $type with value $a.$b but "
                    + "ordinality has changed")
        }

        return fromOrd!!
    }

    override fun writeObject(obj: Any, data: Data, type: Type, output: SerializationOutput) {
        if (obj !is Enum<*>) throw NotSerializableException("Serializing $obj as enum when it isn't")

        data.withDescribed(typeNotation.descriptor) {
            withList {
                data.putObject(obj.name)
                data.putObject(obj.ordinal)
            }
        }
    }
}