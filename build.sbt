ThisBuild / scalaVersion := "2.13.12"
organization := "com.blackrise"
ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
lazy val layers = (project in file("."))
  .settings(
    name := "layers",
  ).enablePlugins(GitVersioning)
coverageEnabled := true
//coverageMinimum := 90
val sparkVersion = "3.3.3"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

val scalaDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
)

libraryDependencies ++= sparkDependencies ++ scalaDependencies