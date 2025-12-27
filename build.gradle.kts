plugins {
    java
    application
    `java-library`
    id("info.solidsoft.pitest") version "1.15.0"
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "com.docbench"
version = "1.0.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_23
    targetCompatibility = JavaVersion.VERSION_23
}

repositories {
    mavenCentral()
    // Oracle Maven repository for SODA and additional dependencies
    maven {
        url = uri("https://maven.oracle.com/")
        content {
            includeGroupByRegex("com\\.oracle.*")
        }
    }
    // Glassfish for javax.json
    maven {
        url = uri("https://repo.maven.apache.org/maven2/")
    }
}

dependencies {
    // CLI Framework
    implementation("info.picocli:picocli:4.7.5")
    annotationProcessor("info.picocli:picocli-codegen:4.7.5")

    // Dependency Injection
    implementation("com.google.inject:guice:7.0.0")

    // Configuration
    implementation("org.yaml:snakeyaml:2.2")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.1")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.16.1")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.16.1")

    // Metrics
    implementation("org.hdrhistogram:HdrHistogram:2.1.12")

    // MongoDB Driver
    implementation("org.mongodb:mongodb-driver-sync:4.11.1")

    // Oracle JDBC & SODA for OSON adapter (Oracle 23ai compatible)
    implementation("com.oracle.database.jdbc:ojdbc11:23.6.0.24.10")
    implementation("com.oracle.database.soda:orajsoda:1.1.29") {
        // Exclude the problematic javax.json reference
        exclude(group = "org.glassfish", module = "javax.json")
    }
    implementation("com.oracle.database.jdbc:ucp:23.6.0.24.10")
    // SODA requires javax.json 1.x (not jakarta.json 2.x)
    // Use the javax.json-api + glassfish implementation combo
    implementation("javax.json:javax.json-api:1.1.4")
    runtimeOnly("org.glassfish:javax.json:1.1.4")

    // JSON Processing
    implementation("jakarta.json:jakarta.json-api:2.1.3")
    implementation("org.eclipse.parsson:parsson:1.1.5")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.11")
    implementation("ch.qos.logback:logback-classic:1.4.14")

    // Testing - JUnit 5
    testImplementation(platform("org.junit:junit-bom:5.10.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.junit.jupiter:junit-jupiter-params")

    // Testing - Assertions & Mocking
    testImplementation("org.assertj:assertj-core:3.25.1")
    testImplementation("org.mockito:mockito-core:5.14.2")
    testImplementation("org.mockito:mockito-junit-jupiter:5.14.2")

    // Testing - TestContainers
    testImplementation("org.testcontainers:testcontainers:1.19.3")
    testImplementation("org.testcontainers:junit-jupiter:1.19.3")
    testImplementation("org.testcontainers:mongodb:1.19.3")
    testImplementation("org.testcontainers:oracle-free:1.19.3")

    // Testing - Architecture Tests
    testImplementation("com.tngtech.archunit:archunit-junit5:1.2.1")

    // Benchmarking
    testImplementation("org.openjdk.jmh:jmh-core:1.37")
    testImplementation("org.openjdk.jmh:jmh-generator-annprocess:1.37")
}

application {
    mainClass.set("com.docbench.cli.DocBenchCommand")
}

tasks.withType<JavaCompile> {
    options.compilerArgs.addAll(listOf(
        "-Aproject=${project.group}/${project.name}"
    ))
}

tasks.test {
    useJUnitPlatform()
    // Required for Mockito with Java 21+
    jvmArgs(
        "-XX:+EnableDynamicAgentLoading",
        "--add-opens", "java.base/java.lang=ALL-UNNAMED",
        "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED"
    )
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = true
    }
}

// Separate integration tests
sourceSets {
    create("integrationTest") {
        java.srcDir("src/integrationTest/java")
        resources.srcDir("src/integrationTest/resources")
        compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output
        runtimeClasspath += sourceSets.main.get().output + sourceSets.test.get().output
    }
}

configurations["integrationTestImplementation"].extendsFrom(configurations.testImplementation.get())
configurations["integrationTestRuntimeOnly"].extendsFrom(configurations.testRuntimeOnly.get())

tasks.register<Test>("integrationTest") {
    description = "Runs integration tests with TestContainers."
    group = "verification"
    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath
    useJUnitPlatform()
    shouldRunAfter(tasks.test)
    // Run integration tests sequentially to avoid overwhelming database connections
    maxParallelForks = 1
    // Required for Mockito with Java 21+
    jvmArgs(
        "-XX:+EnableDynamicAgentLoading",
        "--add-opens", "java.base/java.lang=ALL-UNNAMED",
        "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED"
    )
}

// PIT Mutation Testing
pitest {
    junit5PluginVersion.set("1.2.1")
    targetClasses.set(listOf("com.docbench.*"))
    targetTests.set(listOf("com.docbench.*Test"))
    mutators.set(listOf("DEFAULTS"))
    outputFormats.set(listOf("HTML", "XML"))
    timestampedReports.set(false)
    mutationThreshold.set(60)
    coverageThreshold.set(80)
}

tasks.withType<Jar> {
    manifest {
        attributes(
            "Main-Class" to "com.docbench.cli.DocBenchCommand",
            "Implementation-Title" to project.name,
            "Implementation-Version" to project.version
        )
    }
}

tasks.named("check") {
    dependsOn(tasks.named("integrationTest"))
}
