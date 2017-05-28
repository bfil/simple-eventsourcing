name := "simple-eventsourcing"
version := "0.1.0"
scalaVersion := "2.12.2"
organization := "io.bfil"
organizationName := "Bruno Filippone"
organizationHomepage := Some(url("http://bfil.io"))
scalacOptions ++= Seq(
  "-encoding", "UTF-8", "-deprecation", "-feature", "-unchecked", "-Ywarn-unused-import",
  "-language:implicitConversions", "-language:postfixOps", "-language:higherKinds"
)
javacOptions ++= Seq("-Xlint:deprecation", "-Xlint:unchecked")
libraryDependencies ++= Seq(
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.1.0",
  "io.circe" %% "circe-core" % "0.8.0",
  "io.circe" %% "circe-generic" % "0.8.0",
  "io.circe" %% "circe-parser" % "0.8.0",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
