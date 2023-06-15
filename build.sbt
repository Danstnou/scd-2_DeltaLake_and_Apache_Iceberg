name := "scd-2"

lazy val commonSettings = Seq(
  organization := "com.scd2",
  scalaVersion := "2.12.6"
)

lazy val common = (project in file("common")).settings(commonSettings: _*)
lazy val scd2DeltaLake = (project in file("scd2-DeltaLake")).settings(commonSettings: _*).dependsOn(common)
lazy val scd2Iceberg = (project in file("scd2-Iceberg")).settings(commonSettings: _*).dependsOn(common)
lazy val launch = (project in file("launch")).settings(commonSettings: _*).dependsOn(common, scd2DeltaLake, scd2Iceberg)