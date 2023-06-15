ThisBuild / version := "0.1.0-SNAPSHOT"
name := "scd2-Iceberg"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.3.0"

libraryDependencies += "org.postgresql" % "postgresql" % "42.2.18"
libraryDependencies += "org.apache.iceberg" %% "iceberg-spark-runtime-3.3" % "1.1.0"
libraryDependencies += "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.1"