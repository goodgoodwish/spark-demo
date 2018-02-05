name := "spark01"

version := "0.1"

scalaVersion := "2.11.12"

//libraryDependencies ++= Seq(
//  "net.sourceforge.htmlcleaner" % "htmlcleaner" % "2.4",
//  "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test",
//  "org.foobar" %% "foobar" % "1.8"
//)

// libraryDependencies += groupID % artifactID % revision % configuration
// libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.1"
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.1"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.1"

//libraryDependencies ++= Seq("org.apache.spark" % "spark-sql_2.11" % "2.2.0",
//  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.0",
//  "org.apache.kafka" % "kafka-clients" % "0.11.0.1")

// ivyLoggingLevel := UpdateLogging.Quiet
// logLevel := Level.Error
