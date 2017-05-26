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
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
