name := "spark-refcost2016"

version := "1.0"

scalaVersion := "2.11.11"

lazy val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
