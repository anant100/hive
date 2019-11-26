package ca.mcit.bigdata.hive

import java.sql.{Connection, DriverManager, ResultSet}
import org.apache.hadoop.fs.Path

object HiveClient extends Main with App {

  /** Create OR Replace(If already Exists) Directory to HDFS */
  val outputDir = new Path("/user/cloudera/summer2019/project4/thakkar")
  if (fs.exists(outputDir)) fs.delete(outputDir, true)
  fs.mkdirs(outputDir)

  // Copy data to the staging data
  fs.copyFromLocalFile(new Path("/home/bd-user/Downloads/gtfs_stm/trips.txt"), new Path("/user/cloudera/summer2019/project4/thakkar/trips/trips"))
  fs.copyFromLocalFile(new Path("/home/bd-user/Downloads/gtfs_stm/frequencies.txt"), new Path("/user/cloudera/summer2019/project4/thakkar/frequencies/frequencies"))
  fs.copyFromLocalFile(new Path("/home/bd-user/Downloads/gtfs_stm/calendar_dates.txt"), new Path("/user/cloudera/summer2019/project4/thakkar/calendar_dates/calendar_dates"))

  // Step 1: load the Hive JDBC driver
  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)

  // Step 2: connect to the server
  // A connection requires a connection string. This is equal to what we use in beeline to connect to the Hive server
  val connection: Connection = DriverManager.getConnection("jdbc:hive2://172.16.129.58:10000/thakkar", "cloudera", "cloudera")
  val stmt = connection.createStatement()

  // Step 3: run the query and process the results
  stmt.execute("DROP TABLE IF EXISTS ext_trips")
  stmt.execute("CREATE EXTERNAL TABLE thakkar.ext_trips ( " +
    "route_id INT, " +
    "service_id STRING, " +
    "trip_id STRING, " +
    "trip_headsign STRING, " +
    "direction_id INT, " +
    "shape_id INT, " +
    "wheelchair_accessible INT, " +
    "note_fr STRING, " +
    "note_en STRING " +
    ") " +
    "ROW FORMAT DELIMITED " +
    "FIELDS TERMINATED BY ',' " +
    "STORED AS TEXTFILE " +
    "LOCATION '/user/cloudera/summer2019/project4/thakkar/trips/' " +
    "TBLPROPERTIES ('skip.header.line.count' = '1', 'serialization.null.format' = '') ")

  stmt.execute("DROP TABLE IF EXISTS ext_frequencies")
  stmt.execute("CREATE EXTERNAL TABLE thakkar.ext_frequencies ( " +
    "trip_id STRING, " +
    "start_time STRING, " +
    "end_time STRING, " +
    "headway_secs INT " +
    ") " +
    "ROW FORMAT DELIMITED " +
    "FIELDS TERMINATED BY ',' " +
    "STORED AS TEXTFILE " +
    "LOCATION '/user/cloudera/summer2019/project4/thakkar/frequencies/' " +
    "TBLPROPERTIES ('skip.header.line.count' = '1', 'serialization.null.format' = '') ")

  stmt.execute("DROP TABLE IF EXISTS ext_calendar_dates")
  stmt.execute("CREATE EXTERNAL TABLE thakkar.ext_calendar_dates ( " +
    "service_id STRING, " +
    "date STRING, " +
    "exception_type INT " +
    ") " +
    "ROW FORMAT DELIMITED " +
    "FIELDS TERMINATED BY ',' " +
    "STORED AS TEXTFILE " +
    "LOCATION '/user/cloudera/summer2019/project4/thakkar/calendar_dates/' " +
    "TBLPROPERTIES ('skip.header.line.count' = '1', 'serialization.null.format' = '') ")

  stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict;") // To allow dynamic partitiions permission

  stmt.execute("DROP TABLE IF EXISTS enriched_trip")
  stmt.execute("CREATE TABLE enriched_trip ( " +
    "route_id INT, " +
    "service_id STRING, " +
    "trip_id STRING, " +
    "trip_headsign STRING, " +
    "direction_id INT, " +
    "shape_id INT, " +
    //"wheelchair_accessible INT, " + -- Skip it because of PARTITION
    "note_fr STRING, " +
    "note_en STRING, " +
    //"service_id STRING, " + -- Skip it because of Duplication
    "date STRING, " +
    "exception_type INT, " +
    //"trip_id STRING, " + -- Skip it because of Duplication
    "start_time STRING, " +
    "end_time STRING, " +
    "headway_secs INT " +
    ") " +
    "PARTITIONED BY (wheelchair_accessible INT)" +
    "STORED AS PARQUET " +
    "TBLPROPERTIES('parquet.compression' = 'GZIP') ")

  // Insert Data by Dynamic Partition
  stmt.execute("INSERT OVERWRITE TABLE enriched_trip PARTITION(wheelchair_accessible) " +
    "SELECT t.route_id, t.service_id, t.trip_id, t.trip_headsign, t.direction_id, t.shape_id, t.note_fr, t.note_en, d.date,d.exception_type, f.start_time,f.end_time,f.headway_secs, t.wheelchair_accessible " +
    "FROM ext_trips t " +
    "LEFT JOIN ext_calendar_dates d ON t.service_id = d.service_id " +
    "LEFT JOIN ext_frequencies f ON t.trip_id = f.trip_id")

  // Step 4: close resources
  stmt.close()
  connection.close()
}