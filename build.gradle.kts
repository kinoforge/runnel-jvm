import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.protobuf)
    alias(libs.plugins.publish)
}

val ktlint = configurations.create("ktlint")

dependencies {
    api(libs.grpc.stub)
    api(libs.grpc.kotlin.stub)
    api(libs.protobuf.java)
    api(libs.coroutines.core)
    api(libs.jackson.databind)
    implementation(libs.grpc.protobuf)
    implementation(libs.jackson.kotlin)
    implementation(libs.slf4j.api)
    runtimeOnly(libs.grpc.netty)
    ktlint(libs.ktlint) {
        attributes {
            attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling.EXTERNAL))
        }
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

kotlin {
    explicitApi()
    compilerOptions {
        jvmTarget = JvmTarget.JVM_17
        allWarningsAsErrors = true
        freeCompilerArgs.add("-Xjdk-release=17")
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.release = 17
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${libs.versions.protoc.get()}"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${libs.versions.grpc.get()}"
        }
        create("grpckt") {
            artifact = "io.grpc:protoc-gen-grpc-kotlin:${libs.versions.grpckt.get()}:jdk8@jar"
        }
    }
    generateProtoTasks {
        all().configureEach {
            plugins {
                create("grpc")
                create("grpckt")
            }
        }
    }
}

testing {
    suites {
        named<JvmTestSuite>("test") {
            useJUnitJupiter(libs.versions.junit)
        }
        register<JvmTestSuite>("contractTest") {
            useJUnitJupiter(libs.versions.junit)
            dependencies {
                implementation(project())
                runtimeOnly(libs.grpc.netty)
            }
            targets.configureEach {
                testTask.configure {
                    shouldRunAfter(tasks.named("test"))
                    outputs.upToDateWhen { false }
                }
            }
        }
    }
}

tasks.withType<Test>().configureEach {
    testLogging {
        events("failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

val ktlintSources = listOf("src/**/*.kt", "*.kts", "!**/build/**")

val ktlintCheck =
    tasks.register<JavaExec>("ktlintCheck") {
        group = "verification"
        classpath = ktlint
        mainClass = "com.pinterest.ktlint.Main"
        args(ktlintSources)
    }

tasks.register<JavaExec>("ktlintFormat") {
    group = "formatting"
    classpath = ktlint
    mainClass = "com.pinterest.ktlint.Main"
    jvmArgs("--add-opens=java.base/java.lang=ALL-UNNAMED")
    args(listOf("-F") + ktlintSources)
}

tasks.check {
    dependsOn(ktlintCheck)
}

mavenPublishing {
    publishToMavenCentral()
    if (providers.gradleProperty("signingInMemoryKey").isPresent) signAllPublications()
    coordinates(group.toString(), "runnel", version.toString())
    pom {
        name = "Runnel"
        description = "The Kotlin and Java client and worker for Runnel, the job queue."
        url = "https://github.com/kinoforge/runnel-jvm"
        licenses {
            license {
                name = "Apache-2.0"
                url = "https://www.apache.org/licenses/LICENSE-2.0"
            }
        }
        developers {
            developer {
                id = "kinoforge"
                name = "Kinoforge"
                url = "https://github.com/kinoforge"
            }
        }
        scm {
            url = "https://github.com/kinoforge/runnel-jvm"
            connection = "scm:git:https://github.com/kinoforge/runnel-jvm.git"
        }
    }
}

tasks.withType<Javadoc>().configureEach {
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:none", "-quiet")
}
