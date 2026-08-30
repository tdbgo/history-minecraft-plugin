import java.nio.charset.StandardCharsets
import java.util.jar.Manifest
import java.util.zip.ZipFile

plugins {
    java
    id("com.gradleup.shadow") version "9.2.2"
}

group = "kr.playcity"
version = "0.4.0-alpha.7"

val paperApiVersion = providers.gradleProperty("paperApiVersion")
val javaVersion = providers.gradleProperty("javaVersion").map(String::toInt)

repositories {
    mavenCentral()
    maven {
        name = "papermc"
        url = uri("https://repo.papermc.io/repository/maven-public/")
    }
    maven {
        name = "enginehub"
        url = uri("https://maven.enginehub.org/repo/")
    }
}

dependencies {
    compileOnly("io.papermc.paper:paper-api:${paperApiVersion.get()}")
    compileOnly("com.sk89q.worldedit:worldedit-bukkit:7.4.4")
    compileOnly("com.fastasyncworldedit:FastAsyncWorldEdit-Core:2.15.3")

    implementation("org.xerial:sqlite-jdbc:3.53.2.0") {
        exclude(group = "org.slf4j", module = "slf4j-api")
    }
    implementation("org.postgresql:postgresql:42.7.13") {
        // pgjdbc uses Checker annotations for static analysis only; the server runtime does not need them.
        exclude(group = "org.checkerframework", module = "checker-qual")
    }

    testImplementation(platform("org.junit:junit-bom:5.13.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.papermc.paper:paper-api:${paperApiVersion.get()}")
    testImplementation("com.sk89q.worldedit:worldedit-core:7.4.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.17")
}

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(javaVersion.get()))
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.release.set(javaVersion.get())
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
}

val pluginVersion = version.toString()

tasks.processResources {
    filteringCharset = "UTF-8"
    inputs.property("pluginVersion", pluginVersion)
    filesMatching("plugin.yml") {
        expand("version" to pluginVersion)
    }
}

tasks.jar {
    enabled = false
}

tasks.shadowJar {
    archiveClassifier.set("")
    mergeServiceFiles()
    manifest {
        attributes(
            "Implementation-Title" to "History",
            "Implementation-Version" to pluginVersion,
            "Implementation-Vendor" to "PLAYCITY BLOCK",
            "Implementation-License" to "MIT"
        )
    }
    from(rootProject.file("LICENSE")) {
        into("META-INF")
        rename { "HISTORY-LICENSE.txt" }
    }
    from(rootProject.file("NOTICE")) {
        into("META-INF")
        rename { "HISTORY-NOTICE.txt" }
    }
    from(rootProject.file("THIRD_PARTY_NOTICES.md")) {
        into("META-INF")
    }
    from(rootProject.file("licenses/POSTGRESQL-JDBC-LICENSE.txt")) {
        into("META-INF/licenses")
    }
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}

tasks.build {
    dependsOn(tasks.shadowJar)
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--enable-native-access=ALL-UNNAMED")
}

tasks.withType<AbstractArchiveTask>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}

val shadowArchive = tasks.shadowJar.flatMap { it.archiveFile }

tasks.register("verifyPluginJar") {
    group = "verification"
    description = "Validates the distributable History plugin JAR."
    dependsOn(tasks.shadowJar)
    inputs.file(shadowArchive)

    doLast {
        val archive = shadowArchive.get().asFile
        check(archive.isFile && archive.length() > 0L) { "Plugin JAR is missing: $archive" }

        ZipFile(archive).use { zip ->
            val entries = zip.entries().asSequence().toList()
            val names = entries.map { it.name }.toSet()
            val required = setOf(
                "plugin.yml",
                "config.yml",
                "META-INF/HISTORY-LICENSE.txt",
                "META-INF/HISTORY-NOTICE.txt",
                "META-INF/THIRD_PARTY_NOTICES.md",
                "META-INF/licenses/POSTGRESQL-JDBC-LICENSE.txt",
                "META-INF/licenses/com.ongres.scram/scram-client-3.2/META-INF/LICENSE",
                "META-INF/licenses/com.ongres.scram/scram-common-3.2/META-INF/LICENSE",
                "META-INF/licenses/com.ongres.stringprep/saslprep-2.2/META-INF/LICENSE",
                "META-INF/licenses/com.ongres.stringprep/stringprep-2.2/META-INF/LICENSE",
                "META-INF/maven/org.xerial/sqlite-jdbc/LICENSE",
                "kr/playcity/history/HistoryPlugin.class",
                "org/sqlite/JDBC.class",
                "org/postgresql/Driver.class"
            )
            check(names.containsAll(required)) { "Plugin JAR is missing: ${required - names}" }
            check(names.none {
                it.startsWith("org/bukkit/")
                    || it.startsWith("io/papermc/paper/")
                    || it.startsWith("com/sk89q/worldedit/")
                    || it.startsWith("com/fastasyncworldedit/")
                    || it.startsWith("net/coreprotect/")
            }) {
                "Server, WorldEdit, FAWE, or CoreProtect classes must not be bundled"
            }

            val descriptor = zip.getInputStream(zip.getEntry("plugin.yml")).bufferedReader().use { it.readText() }
            check(!descriptor.contains("\${version}")) { "plugin.yml version was not expanded" }
            check(descriptor.contains("version: '$pluginVersion'")) { "plugin.yml version mismatch" }

            val manifest = Manifest(zip.getInputStream(zip.getEntry("META-INF/MANIFEST.MF")))
            check(manifest.mainAttributes.getValue("Implementation-License") == "MIT") {
                "JAR manifest must record the MIT project license"
            }

            entries.filterNot { it.isDirectory }.forEach { entry ->
                zip.getInputStream(entry).use { input ->
                    val bytes = input.readBytes()
                    if (entry.name.endsWith(".class")) {
                        val constantPoolText = String(bytes, StandardCharsets.ISO_8859_1)
                        check(!constantPoolText.contains("net/coreprotect")) {
                            "History must not link to CoreProtect: ${entry.name}"
                        }
                        if (entry.name.startsWith("kr/playcity/history/rollback/")) {
                            check(!constantPoolText.contains("com/sk89q/worldedit")
                                && !constantPoolText.contains("com/fastasyncworldedit")) {
                                "Rollback execution must not link to WorldEdit or FAWE: ${entry.name}"
                            }
                            check(!constantPoolText.contains("getChunksAtAsync")
                                && !constantPoolText.contains("setChunkForceLoaded")
                                && !constantPoolText.contains("regenerateChunk")
                                && !constantPoolText.contains("getIntersectingChunks")) {
                                "Rollback execution must not expose broad or force-loaded chunk APIs: ${entry.name}"
                            }
                        }
                    }
                    // Fully reading each entry also verifies its CRC.
                }
            }
        }
    }
}

tasks.check {
    dependsOn(tasks.named("verifyPluginJar"))
}
