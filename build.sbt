name := "akka-xml-stream"

organization := "drewhk.com"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.8"

val AkkaVersion = "2.4.8"

resolvers += Resolver.typesafeRepo("releases")

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % AkkaVersion

libraryDependencies += "com.typesafe.akka" %% "akka-stream" % AkkaVersion

libraryDependencies += "com.fasterxml" % "aalto-xml" % "1.0.0"
