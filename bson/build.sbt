name := "gagool-bson"


libraryDependencies ++= Seq(
  "org.mongodb" % "mongodb-driver-reactivestreams" % "5.5.1"
)

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.19" % Test
)
