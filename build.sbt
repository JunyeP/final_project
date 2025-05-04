name := "FinalProject"
version := "1.0"
scalaVersion := "2.12.10"    // or match your Spark's Scala version
resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1" % "provided",
  "org.apache.spark" %% "spark-graphx" % "3.5.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided"
)