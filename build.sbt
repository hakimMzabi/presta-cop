name := "projet"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.4.0"
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.7.4"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.9"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.13.3"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.0.0" % "2.3.0"
libraryDependencies +="com.github.daddykotex" %% "courier" % "2.0.0"

