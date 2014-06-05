libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.7" % "test"

sourcesInBase := false

scalaSource in Compile := baseDirectory.value / "src-main-scala"

scalaSource in Test := baseDirectory.value / "src-test-scala"

