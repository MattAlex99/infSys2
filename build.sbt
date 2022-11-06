ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "aufgabe2"
  )


libraryDependencies += "redis.clients" % "jedis" % "4.3.1"
