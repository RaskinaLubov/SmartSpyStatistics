name := "SmartSpy"

version := "1.0"

scalaVersion := "2.11.7"


libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.0.9",
  "org.apache.spark" %% "spark-core" % "1.5.2",
  "org.apache.spark" %% "spark-sql" % "1.5.2",
  "joda-time" % "joda-time" % "2.0"
)