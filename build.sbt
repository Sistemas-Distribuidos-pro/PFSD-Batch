ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.12.19"

lazy val sparkVersion = "3.5.1"
lazy val hadoopVersion = "3.3.4"

lazy val root = (project in file("."))
  .settings(
    name := "pfsd-batch-analytics",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
      "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262"
    ),
    Compile / run / fork := true,
    Compile / mainClass := Some("com.pfsd.batch.BatchAnalyticsJob")
  )
