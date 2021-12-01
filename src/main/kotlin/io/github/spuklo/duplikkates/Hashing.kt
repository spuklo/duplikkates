package io.github.spuklo.duplikkates

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.javadsl.AbstractBehavior
import akka.actor.typed.javadsl.ActorContext
import akka.actor.typed.javadsl.Behaviors
import akka.actor.typed.javadsl.Receive
import akka.stream.javadsl.FileIO
import akka.stream.javadsl.Sink
import java.security.MessageDigest
import java.io.File as JavaIoFile

sealed class HashingResultProtocol
data class HashAndFile(val hash: List<Byte>, val file: JavaIoFile) : HashingResultProtocol()
object HashingResultPoisonPill : HashingResultProtocol()

class HashingResultActor(context: ActorContext<HashingResultProtocol>) :
    AbstractBehavior<HashingResultProtocol>(context) {

    companion object {
        fun create(): Behavior<HashingResultProtocol> = Behaviors.setup { ctx -> HashingResultActor(ctx) }
    }

    override fun createReceive(): Receive<HashingResultProtocol> {
        return hashingResults(mapOf())
    }

    private fun hashingResults(results: Map<List<Byte>, List<JavaIoFile>>): Receive<HashingResultProtocol> {
        return newReceiveBuilder()
            .onMessage(HashAndFile::class.java) { m ->
                hashingResults(
                    mapOf(m.hash to results.getOrDefault(m.hash, listOf()).plus(m.file))
                        .plus(results.filter { it.key != m.hash })
                )
            }
            .onMessage(HashingResultPoisonPill::class.java) {
                results.filter { it.value.size > 1 }
                    .also {
                        context.log.info("Found {} conflicts, total of {} files.", it.size, it.values.sumOf { list -> list.size })
                        it.forEach { (k, v) ->
                            context.log.info("Hash: {}: {}", k.joinToString("") { b -> "%02x".format(b) }, v)
                        }
                    }
                context.system.terminate()
                Behaviors.stopped()
            }
            .build()
    }
}

sealed class HashingProtocol
data class FileToHash(val file: JavaIoFile) : HashingProtocol()
data class HashResult(val hash: List<Byte>, val file: JavaIoFile) : HashingProtocol()
data class FailedHash(val throwable: Throwable) : HashingProtocol()
object HashingPoisonPill : HashingProtocol()

class HashingActor(context: ActorContext<HashingProtocol>, private val resultActor: ActorRef<HashingResultProtocol>) :
    AbstractBehavior<HashingProtocol>(context) {

    companion object {
        fun create(hashingResultActor: ActorRef<HashingResultProtocol>): Behavior<HashingProtocol> =
            Behaviors.setup { ctx -> HashingActor(ctx, hashingResultActor) }
    }

    override fun createReceive(): Receive<HashingProtocol> {
        return hashAndWaitForResults(false, 0, true)
    }

    private fun hashAndWaitForResults(
        isStopping: Boolean,
        counter: Int,
        shouldSentShutdown: Boolean
    ): Receive<HashingProtocol> = newReceiveBuilder()
        .onMessage(FileToHash::class.java) { m ->
            context.pipeToSelf(
                FileIO.fromFile(m.file)
                    .fold(MessageDigest.getInstance("SHA-256")) { md, bytes ->
                        md.update(bytes.asByteBuffer())
                        md
                    }
                    .map { md -> md.digest() }
                    .runWith(Sink.head(), context.system)
            ) { byteArray, throwable -> throwable?.let(::FailedHash) ?: HashResult(byteArray.toList(), m.file) }
            hashAndWaitForResults(isStopping, counter + 1, shouldSentShutdown)
        }
        .onMessage(FailedHash::class.java) {
            shutdownIfNeeded(isStopping, counter, shouldSentShutdown)
        }
        .onMessage(HashResult::class.java) { m ->
            resultActor tell HashAndFile(m.hash, m.file)
            shutdownIfNeeded(isStopping, counter, shouldSentShutdown)
        }
        .onMessage(HashingPoisonPill::class.java) {
            if (counter == 0 && shouldSentShutdown) {
                resultActor tell HashingResultPoisonPill
                return@onMessage hashAndWaitForResults(true, counter, false)
            } else {
                hashAndWaitForResults(true, counter, shouldSentShutdown)
            }
        }
        .build()

    private fun shutdownIfNeeded(
        isStopping: Boolean,
        counter: Int,
        shouldSentShutdown: Boolean
    ): Receive<HashingProtocol> {
        if (isStopping) {
            context.self tell HashingPoisonPill
        }
        return hashAndWaitForResults(isStopping, counter - 1, shouldSentShutdown)
    }
}
