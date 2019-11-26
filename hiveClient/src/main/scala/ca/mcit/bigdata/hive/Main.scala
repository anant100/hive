package ca.mcit.bigdata.hive

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait Main {
  val conf = new Configuration()
  conf.addResource(new Path("/home/bd-user/opt/hadoop/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/home/bd-user/opt/hadoop/etc/cloudera/hdfs-site.xml"))

  //Set dynamic value by using this --> conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020")
  val fs = FileSystem.get(conf)
}