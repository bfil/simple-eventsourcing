lazy val root = Project("simple-eventsourcing", file("."))
  .settings(settings, publishArtifact := false)
  .aggregate(core, mongo)

lazy val core = Project("simple-eventsourcing-core", file("simple-eventsourcing-core"))
  .settings(settings, name := "simple-eventsourcing", libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  ))

lazy val mongo = Project("simple-eventsourcing-mongo", file("simple-eventsourcing-mongo"))
  .settings(settings, libraryDependencies ++= Seq(
    "org.mongodb.scala" %% "mongo-scala-driver" % "2.1.0",
    "io.circe" %% "circe-core" % "0.8.0",
    "io.circe" %% "circe-generic" % "0.8.0",
    "io.circe" %% "circe-parser" % "0.8.0"
  ))
  .dependsOn(core)

lazy val settings = Seq(
  scalaVersion := "2.12.2",
  crossScalaVersions := Seq("2.12.2", "2.11.8"),
  organization := "io.bfil",
  organizationName := "Bruno Filippone",
  organizationHomepage := Some(url("http://bfil.io")),
  homepage := Some(url("https://github.com/bfil/simple-eventsourcing")),
  licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
  developers := List(
    Developer("bfil", "Bruno Filippone", "bruno@bfil.io", url("http://bfil.io"))
  ),
  startYear := Some(2015),
  publishTo := Some("Bintray" at s"https://api.bintray.com/maven/bfil/maven/${name.value}"),
  credentials += Credentials(Path.userHome / ".ivy2" / ".bintray-credentials"),
  scmInfo := Some(ScmInfo(
    url(s"https://github.com/bfil/simple-eventsourcing"),
    s"git@github.com:bfil/simple-eventsourcing.git"
  ))
)
