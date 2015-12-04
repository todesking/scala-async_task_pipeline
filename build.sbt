organization := "com.todesking"

name := "async_task_pipeline"

version := "0.0.10-SNAPSHOT"

scalaVersion := "2.11.7"

publishTo := Some(Resolver.file("com.todesking",file("./repo/"))(Patterns(true, Resolver.mavenStyleBasePattern)))

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.1.5"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.7" % "test"

sourcesInBase := false

scalaSource in Compile := baseDirectory.value / "src-main-scala"

scalaSource in Test := baseDirectory.value / "src-test-scala"

scalacOptions ++= Seq(
  "-deprecation", "-feature"
)
