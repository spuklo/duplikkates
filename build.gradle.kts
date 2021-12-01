plugins {
    kotlin("jvm") version "1.6.0"
}

group = "io.github.spuklo"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val versions = mapOf(
    "scalaBinary" to "2.13",
    "akkaTyped" to "2.6.17",
    "logback" to "1.2.7",
    "alpakkaVersion" to "3.0.3"
)

dependencies {
    implementation(kotlin("stdlib"))
    implementation("com.typesafe.akka:akka-actor-typed_${versions["scalaBinary"]}:${versions["akkaTyped"]}")
    implementation("com.typesafe.akka:akka-stream-typed_${versions["scalaBinary"]}:${versions["akkaTyped"]}")
    implementation("com.lightbend.akka:akka-stream-alpakka-file_${versions["scalaBinary"]}:${versions["alpakkaVersion"]}")
    implementation("ch.qos.logback:logback-classic:${versions["logback"]}")
}