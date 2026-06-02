name := "gagool-core"

val kyoVersion = "1.0.0-RC2"

libraryDependencies ++= Seq(
  "io.getkyo" %% "kyo-core" % kyoVersion,
  "io.getkyo" %% "kyo-reactive-streams" % kyoVersion,
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "4.21.0" % Test
)
