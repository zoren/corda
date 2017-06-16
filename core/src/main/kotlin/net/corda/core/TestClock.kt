package net.corda.core

import net.corda.core.serialization.SerializeAsToken
import net.corda.core.serialization.SerializeAsTokenContext
import net.corda.core.serialization.SingletonSerializationToken
import net.corda.core.serialization.SingletonSerializationToken.Companion.singletonSerializationToken
import net.corda.node.utilities.MutableClock
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import javax.annotation.concurrent.ThreadSafe

/**
 * A [Clock] that can have the time advanced for use in testing. Set the "useTestClock" configuration in the node's conf
 * file to "true" to enable this clock.
 */
@ThreadSafe
class TestClock(private var delegateClock: Clock = Clock.systemUTC()) : MutableClock(), SerializeAsToken {

    private val token = singletonSerializationToken(javaClass)

    override fun toToken(context: SerializeAsTokenContext) = token.registerWithContext(context, this)

    /**
     * Advance this [Clock] by the specified [Duration] for testing purposes.
     */
    @Synchronized fun advanceBy(duration: Duration) {
        delegateClock = offset(delegateClock, duration)
        notifyMutationObservers()
    }

    @Synchronized fun updateDate(date: LocalDate): Boolean {
        val currentDate = LocalDate.now(this)
        if (currentDate.isBefore(date)) {
            // It's ok to increment
            delegateClock = Clock.offset(delegateClock, Duration.between(currentDate.atStartOfDay(), date.atStartOfDay()))
            notifyMutationObservers()
            return true
        }
        return false
    }

    /**
     * Move this [Clock] to the specified [Instant] for testing purposes.
     *
     * This will only be approximate due to the time ticking away, but will be some time shortly after the requested [Instant].
     */
    @Synchronized fun setTo(newInstant: Instant) = advanceBy(Duration.between(instant(), newInstant))

    @Synchronized override fun instant(): Instant {
        return delegateClock.instant()
    }

    @Deprecated("Do not use this. Instead seek to use ZonedDateTime methods.", level = DeprecationLevel.ERROR)
    override fun withZone(zone: ZoneId): Clock {
        throw UnsupportedOperationException("Tokenized clock does not support withZone()")
    }

    @Synchronized override fun getZone(): ZoneId {
        return delegateClock.zone
    }
}
