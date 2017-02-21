package net.corda.core

import org.assertj.core.api.Assertions.*
import org.junit.After
import org.junit.Ignore
import org.junit.Test
import rx.subjects.PublishSubject
import java.util.*
import java.util.concurrent.CancellationException
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors

@Ignore
class UtilsTest {
    private val executor = Executors.newSingleThreadExecutor()

    @After
    fun cleanUp() {
        executor.shutdownNow()
    }

    @Test
    fun `future - value`() {
        val f = future { "Hello" }
        var value: Any? = null
        f.addListener(Runnable { value = f.get() }, executor)
        assertThat(f.get()).isEqualTo("Hello")
        assertThat(value).isEqualTo("Hello")
    }

    @Test
    fun `future - exception`() {
        val thrownException = Exception()
        val f = future { throw thrownException }
        var caughtException: Throwable? = null
        f.addListener(Runnable {
            try {
                f.get()
            } catch (e: ExecutionException) {
                caughtException = e.cause
            }
        }, executor)

        assertThatThrownBy {
            f.get()
        }.hasCause(thrownException)
        assertThat(caughtException).isSameAs(thrownException)
    }

    @Test
    fun `Observable toFuture - single item observable`() {
        val subject = PublishSubject.create<String>()
        val future = subject.toFuture()
        subject.onNext("Hello")
        assertThat(future.getOrThrow()).isEqualTo("Hello")
    }

    @Test
    fun `Observable toFuture - empty obserable`() {
        val subject = PublishSubject.create<String>()
        val future = subject.toFuture()
        subject.onCompleted()
        assertThatExceptionOfType(NoSuchElementException::class.java).isThrownBy {
            future.getOrThrow()
        }
    }

    @Test
    fun `Observable toFuture - more than one item observable`() {
        val subject = PublishSubject.create<String>()
        val future = subject.toFuture()
        subject.onNext("Hello")
        subject.onNext("World")
        subject.onCompleted()
        assertThat(future.getOrThrow()).isEqualTo("Hello")
    }

    @Test
    fun `Observable toFuture - erroring observable`() {
        val subject = PublishSubject.create<String>()
        val future = subject.toFuture()
        val exception = Exception("Error")
        subject.onError(exception)
        assertThatThrownBy {
            future.getOrThrow()
        }.isSameAs(exception)
    }

    @Test
    fun `Observable toFuture - cancel`() {
        val subject = PublishSubject.create<String>()
        val future = subject.toFuture()
        future.cancel(false)
        assertThat(subject.hasObservers()).isFalse()
        subject.onNext("Hello")
        assertThatExceptionOfType(CancellationException::class.java).isThrownBy {
            future.get()
        }
    }
}