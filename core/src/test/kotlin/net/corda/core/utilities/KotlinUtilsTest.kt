package net.corda.core.utilities

import net.corda.core.crypto.random63BitValue
import net.corda.core.serialization.CordaSerializable
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.testing.initialiseTestSerialization
import net.corda.testing.resetTestSerialization
import org.assertj.core.api.Assertions.assertThat
import org.junit.After
import org.junit.Before
import org.junit.Test

class KotlinUtilsTest {
    @Before
    fun setup() {
        initialiseTestSerialization()
    }

    @After
    fun teadDown() {
        resetTestSerialization()
    }

    @Test
    fun `transient property which is null`() {
        val test = NullTransientProperty()
        test.transientValue
        test.transientValue
        assertThat(test.evalCount).isEqualTo(1)
    }

    @Test
    fun `transient property with non-capturing lamba`() {
        val original = NonCapturingTransientProperty()
        val originalVal = original.transientVal
        val copy = original.serialize().deserialize()
        val copyVal = copy.transientVal
        assertThat(copyVal).isNotEqualTo(originalVal)
        assertThat(copy.transientVal).isEqualTo(copyVal)
    }

    @Test
    fun `transient property with capturing lamba`() {
        val original = CapturingTransientProperty("Hello")
        val originalVal = original.transientVal
        val copy = original.serialize().deserialize()
        val copyVal = copy.transientVal
        assertThat(copyVal).isNotEqualTo(originalVal)
        assertThat(copy.transientVal).isEqualTo(copyVal)
        assertThat(copy.transientVal).startsWith("Hello")
    }

    private class NullTransientProperty {
        var evalCount = 0
        val transientValue by transient {
            evalCount++
            null
        }
    }

    @CordaSerializable
    private class NonCapturingTransientProperty {
        val transientVal by transient { random63BitValue() }
    }

    @CordaSerializable
    private class CapturingTransientProperty(prefix: String) {
        private val seed = random63BitValue()
        val transientVal by transient { prefix + seed + random63BitValue() }
    }
}