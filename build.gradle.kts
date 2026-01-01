plugins {
    java
}

group = "com.docbench"
version = "1.0.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

repositories {
    mavenCentral()
}

dependencies {
    // MongoDB Driver
    implementation("org.mongodb:mongodb-driver-sync:4.11.1")

    // Oracle JDBC (includes native JSON/OSON support)
    implementation("com.oracle.database.jdbc:ojdbc11:23.6.0.24.10")

    // Logging (required by MongoDB driver)
    implementation("org.slf4j:slf4j-api:2.0.11")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.11")

    // Testing - JUnit 5
    testImplementation(platform("org.junit:junit-bom:5.10.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")

    // Testing - Assertions
    testImplementation("org.assertj:assertj-core:3.25.1")
}

tasks.test {
    useJUnitPlatform()
}

// Integration tests source set
sourceSets {
    create("integrationTest") {
        java.srcDir("src/integrationTest/java")
        resources.srcDir("src/integrationTest/resources")
        compileClasspath += sourceSets.main.get().output
        runtimeClasspath += sourceSets.main.get().output
    }
}

configurations["integrationTestImplementation"].extendsFrom(configurations.implementation.get())
configurations["integrationTestImplementation"].extendsFrom(configurations.testImplementation.get())
configurations["integrationTestRuntimeOnly"].extendsFrom(configurations.runtimeOnly.get())

tasks.register<Test>("integrationTest") {
    description = "Runs client-side O(n) vs O(1) benchmark tests."
    group = "verification"
    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = true
    }
}
