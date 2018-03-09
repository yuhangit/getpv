name := "GetPV"

version := "0.1"

scalaVersion := "2.11.8"

sbtVersion := "1.0.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" % Provided ,
  "org.apache.spark" %% "spark-sql" % "2.2.0" % Provided
)


//unmanagedBase := baseDirectory.value / "libs"