package io.github.spuklo.duplikkates

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.javadsl.Behaviors
import akka.stream.typed.javadsl.ActorSink

infix fun <T> ActorRef<T>.tell(m: T) = this.tell(m)

object Main {

    @JvmStatic
    fun main(args: Array<String>) {
        val extensions = listOf("jpg", "jpeg", "nef", "raf", "dng", "arw", "crw", "cr2", "cr3")
        val directoryToSearch = pathOrCurrent(args)
        val extensionSensitiveDuplicates = System.getProperty("extSensitive", "true").toBoolean()

        ActorSystem.create(Behaviors.setup<Void> { ctx ->
            val hashingResultActor = ctx.spawn(HashingResultActor.create(), "HashingResultActor")
            val hashingActor = ctx.spawn(HashingActor.create(hashingResultActor), "HashingActor")
            val filesActor: ActorRef<FileProtocol> =
                ctx.spawn(FilesActor.create(extensionSensitiveDuplicates, hashingActor), "FileActor")

            ctx.log.info("Scanning {} for duplicates of files: {}", directoryToSearch, extensions)
            scanDirectory(directoryToSearch, extensions)
                .runWith(ActorSink.actorRef(filesActor, FilePoisonPill, ::FileFail), ctx.system)
            Behaviors.empty()
        }, "DuplikkatesRootActor")
    }
}
