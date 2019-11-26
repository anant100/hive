name := "hiveClient"

version := "0.1"

scalaVersion := "2.13.1"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-hdfs
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0"

// https://mvnrepository.com/artifact/org.apache.hive/hive-jdbc
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.1.0-cdh5.16.2"

// Cloudera artifacts are published in their own remote repository
resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"