name := "gagool-core"

libraryDependencies ++= Seq(
  "co.fs2" %% "fs2-reactive-streams" % "3.12.2",
  "org.typelevel" %% "cats-effect" % "3.6.3",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "4.21.0" % Test
)
