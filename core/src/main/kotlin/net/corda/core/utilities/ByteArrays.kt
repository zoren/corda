@file:JvmName("ByteArrays")

package net.corda.core.utilities

import com.google.common.io.BaseEncoding
import net.corda.core.serialization.CordaSerializable
import java.io.ByteArrayInputStream
import java.util.*

interface ByteSequence {
    val bytes: ByteArray
    val size: Int
    val offset: Int
    fun open(): ByteArrayInputStream
    fun subSequence(offset: Int, size: Int): ByteSequence

    companion object {
        fun of(bytes: ByteArray, offset: Int = 0, size: Int = bytes.size): ByteSequence = if (offset == 0 && size == bytes.size) OpaqueBytes(bytes) else OpaqueByteSequence(bytes, offset, size)
    }

    fun first(n: Int): ByteSequence {
        check(size >= n)
        return subSequence(offset, n)
    }
}

/**
 * A simple class that wraps a byte array and makes the equals/hashCode/toString methods work as you actually expect.
 * In an ideal JVM this would be a value type and be completely overhead free. Project Valhalla is adding such
 * functionality to Java, but it won't arrive for a few years yet!
 */
@CordaSerializable
open class OpaqueBytes(override val bytes: ByteArray) : ByteSequence {
    companion object {
        @JvmStatic
        fun of(vararg b: Byte) = OpaqueBytes(byteArrayOf(*b))
    }

    init {
        check(bytes.isNotEmpty())
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other is OpaqueBytes) return Arrays.equals(bytes, other.bytes)
        return other?.equals(this) ?: false
    }

    override fun hashCode() = Arrays.hashCode(bytes)
    override fun toString() = "[" + bytes.toHexString() + "]"

    override val size: Int get() = bytes.size
    override val offset: Int
        get() = 0

    /** Returns a [ByteArrayInputStream] of the bytes */
    override fun open() = ByteArrayInputStream(bytes)

    override fun subSequence(offset: Int, size: Int): ByteSequence {
        check(offset >= 0)
        check(offset + size <= this.size)
        return OpaqueByteSequence(bytes, offset, size)
    }
}

fun ByteArray.opaque(): OpaqueBytes = OpaqueBytes(this)
fun ByteArray.sequence(offset: Int, size: Int) = ByteSequence.of(this, offset, size)
fun ByteArray.toHexString(): String = BaseEncoding.base16().encode(this)
fun String.parseAsHex(): ByteArray = BaseEncoding.base16().decode(this)

class OpaqueByteSequence(override val bytes: ByteArray, override val offset: Int, override val size: Int) : ByteSequence {
    init {
        check(offset + size <= bytes.size)
        check(offset >= 0)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ByteSequence) return false
        if (this.size != other.size) return false
        return subArraysEqual(this.bytes, this.offset, this.size, other.bytes, other.offset)
    }

    private fun subArraysEqual(a: ByteArray, aOffset: Int, length: Int, b: ByteArray, bOffset: Int): Boolean {
        if (aOffset + length > a.size || bOffset + length > b.size) throw IndexOutOfBoundsException()
        var bytesRemaining = length
        var aPos = aOffset
        var bPos = bOffset
        while (bytesRemaining-- > 0) {
            if (a[aPos++] != b[bPos++]) return false
        }
        return true
    }

    override fun hashCode(): Int {
        var result = 1
        for (index in offset until (offset + size)) {
            result = 31 * result + bytes[index]
        }
        return result
    }

    override fun toString() = opaqueCopy().toString()

    fun opaqueCopy(): OpaqueBytes = OpaqueBytes(Arrays.copyOfRange(bytes, offset, offset + size))

    /** Returns a [ByteArrayInputStream] of the bytes */
    override fun open() = ByteArrayInputStream(bytes, offset, size)

    override fun subSequence(offset: Int, size: Int): ByteSequence {
        check(offset >= this.offset)
        check(offset + size <= this.offset + this.size)
        return OpaqueByteSequence(bytes, offset, size)
    }
}