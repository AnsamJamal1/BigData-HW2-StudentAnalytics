ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "StudentAnalytics",
    libraryDependencies ++= Seq(
      "org.mongodb.scala" %% "mongo-scala-driver" % "5.2.1",
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "org.mongodb.spark" %% "mongo-spark-connector" % "10.3.0"
    )
  )