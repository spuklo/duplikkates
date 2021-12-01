package io.github.spuklo.duplikkates

import akka.NotUsed
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.javadsl.AbstractBehavior
import akka.actor.typed.javadsl.ActorContext
import akka.actor.typed.javadsl.Behaviors
import akka.actor.typed.javadsl.Receive
import akka.stream.javadsl.Source
import akka.stream.javadsl.StreamConverters
import java.nio.file.Files
import java.nio.file.Path
import java.util.Optional
import java.io.File as JavaIoFile

fun JavaIoFile.extension(): String = this.name.split(".").let { it[it.size - 1] }

fun pathOrCurrent(args: Array<String>): Path = Path.of(
    when {
        args.isNotEmpty() -> args[0]
        else -> System.getProperty("user.dir")
    }
)

fun scanDirectory(rootDirectory: Path, extensions: List<String>): Source<FileProtocol, NotUsed> =
    StreamConverters.fromJavaStream { Files.walk(rootDirectory) }
        .map { it.toFile() }
        .filter { !it.isDirectory }
        .filter { withExpectedExtension(it, extensions) }
        .map { File(it) }

private fun withExpectedExtension(file: JavaIoFile, extensions: List<String>) =
    file.name.lowercase().split(".").let { nameSplit ->
        when {
            nameSplit.isEmpty() -> false
            else -> extensions.contains(nameSplit[nameSplit.size - 1])
        }
    }

sealed class FileProtocol
data class File(val file: JavaIoFile) : FileProtocol()
data class FileFail(val throwable: Throwable) : FileProtocol()
object FilePoisonPill : FileProtocol()

class FilesActor(
    context: ActorContext<FileProtocol>,
    private val extensionSensitiveDuplicates: Boolean,
    private val hashingActor: ActorRef<HashingProtocol>
) : AbstractBehavior<FileProtocol>(context) {

    companion object {
        fun create(
            extensionSensitiveDuplicates: Boolean,
            hashingActor: ActorRef<HashingProtocol>
        ): Behavior<FileProtocol> =
            Behaviors.setup { ctx -> FilesActor(ctx, extensionSensitiveDuplicates, hashingActor) }
    }

    private data class FileTrace(val size: Long, val extension: Optional<String>)

    private val fileTrace = { file: JavaIoFile ->
        when {
            extensionSensitiveDuplicates -> FileTrace(file.length(), Optional.of(file.extension()))
            else -> FileTrace(file.length(), Optional.empty())
        }
    }

    override fun createReceive(): Receive<FileProtocol> = handleWithCounter(mapOf(), 0)

    private fun handleWithCounter(seenFiles: Map<FileTrace, List<JavaIoFile>>, counter: Int): Receive<FileProtocol> {
        if (counter > 0 && counter.mod(100) == 0) {
            context.log.info("Processed $counter files")
        }
        return newReceiveBuilder()
            .onMessage(File::class.java) { m ->
                val trace = fileTrace(m.file)
                when {
                    seenFiles.containsKey(trace) -> {
                        val newList = seenFiles.getValue(trace).plus(m.file)
                        if (newList.size == 2) {
                            newList.forEach { hashingActor tell FileToHash(it) }
                        } else {
                            hashingActor tell FileToHash(m.file)
                        }
                        handleWithCounter(
                            mapOf(trace to newList).plus(seenFiles.filter { (k, _) -> k != trace }),
                            counter + 1
                        )
                    }
                    else -> handleWithCounter(seenFiles.plus(trace to listOf(m.file)), counter + 1)
                }
            }
            .onMessageEquals(FilePoisonPill) {
                hashingActor tell HashingPoisonPill
                context.log.info("Finished searching, scanned $counter files.")
                Behaviors.stopped()
            }
            .onMessage(FileFail::class.java) { f ->
                context.log.error("Failure, exiting. {}", f.throwable.message)
                hashingActor tell HashingPoisonPill
                Behaviors.stopped()
            }
            .build()
    }
}