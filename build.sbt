// Root build.sbt - Common Settings and Inter-Module Wiring

ThisBuild / organization := "com.github.h-ayat"
ThisBuild / version := "0.2.5"
ThisBuild / scalaVersion := "3.3.6"
ThisBuild / semanticdbEnabled := true

// Common scalac options
val isCI = sys.env.get("CI").contains("true")

val baseScalacOptions = Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-language:strictEquality",
  "-language:implicitConversions",
  "-Ykind-projector:underscores",
  "-release:17"
)

val ciScalacOptions = Seq(
  "-Xfatal-warnings",
  "-Yno-decode-stacktraces"
)

val devScalacOptions = Seq(
  "-explain",
  "-explain-types",
  "-source:future"
)

ThisBuild / scalacOptions := {
  if (isCI) baseScalacOptions ++ ciScalacOptions
  else baseScalacOptions ++ devScalacOptions
}

// Common test settings
ThisBuild / Test / fork := true
ThisBuild / Test / javaOptions ++= Seq("-Xmx2G", "-Xss4M")



lazy val bson = (project in file("bson"))
lazy val core = (project in file("core")).dependsOn(bson)
lazy val root = (project in file("."))
  .aggregate(bson, core)
  .settings(
    name := "gagool-root",
    publish / skip := true
  )
