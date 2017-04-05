package net.corda.node.services.config

import net.corda.nodeapi.User
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.Test
import java.nio.file.Paths

class FullNodeConfigurationTest {
    @Test
    fun `missing permissions for RPC user`() {
        val config = createConfig("rpcUsers" to listOf(
                mapOf(
                        "username" to "user1",
                        "password" to "pass"
                )
        ))
        assertThat(config.rpcUsers).containsOnly(User("user1", "pass", emptySet()))
    }

    @Test
    fun `Artemis special characters not permitted in RPC usernames`() {
        fun configWithRPCUsername(username: String): FullNodeConfiguration {
            return createConfig("rpcUsers" to listOf(
                    mapOf(
                            "username" to username,
                            "password" to "password"
                    )
            ))
        }

        assertThatThrownBy { configWithRPCUsername("user.1") }.hasMessageContaining(".")
        assertThatThrownBy { configWithRPCUsername("user*1") }.hasMessageContaining("*")
        assertThatThrownBy { configWithRPCUsername("user#1") }.hasMessageContaining("#")
    }

    private fun createConfig(vararg configOverrides: Pair<String, Any?>): FullNodeConfiguration {
        val baseDirectory = Paths.get(".")
        return FullNodeConfiguration(baseDirectory, ConfigHelper.loadConfig(
                baseDirectory = baseDirectory,
                allowMissingConfig = true,
                configOverrides = configOverrides.toMap()
        ))
    }
}
