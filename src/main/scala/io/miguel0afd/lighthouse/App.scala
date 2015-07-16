package io.miguel0afd.lighthouse

import com.datastax.driver.core.{Row, Session, Cluster}
import com.stratio.crossdata.fuse.CrossdataSQLContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkContext, SparkConf}

// One method for defining the schema of an RDD is to make a case class with the desired column
// names and types.
case class Record(key: Int, value: String)

object MapDataframe {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Lighthouse").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext.implicits._

    val df = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i"))).toDF()
    // Any RDD containing case classes can be registered as a table.  The schema of the table is
    // automatically inferred using scala reflection.
    df.registerTempTable("records")

    // Once tables have been registered, you can run SQL queries over them.
    println("Result of SELECT *:")
    sqlContext.sql("SELECT * FROM records").collect().foreach(println)

    sc.stop()
  }
}

object CassandraReadRDD {
  def main(args: Array[String]) {
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")

    val sc = new SparkContext("local[4]", "Lighthouse", conf)

    import com.datastax.spark.connector._

    val rdd = sc.cassandraTable("highschool", "students")
    print("Result of count: ")
    println(rdd.count)

    sc.stop()
  }
}

object CassandraReadDataframeDefault {

  def main(args: Array[String]) {
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")

    val sc = new SparkContext("local[4]", "Lighthouse", conf)

    val sqlContext = new SQLContext(sc)

    sqlContext.sql(
      """CREATE TEMPORARY TABLE students
        |USING org.apache.spark.sql.cassandra
        |OPTIONS (
        |  keyspace "highschool",
        |  table "students",
        |  cluster "Test Cluster",
        |  pushdown "true"
        |)""".stripMargin)

    // Once tables have been registered, you can run SQL queries over them.
    println("Result of SELECT *:")

    val t0 = System.currentTimeMillis

    val result = sqlContext.sql("SELECT * FROM students").collect

    val t1 = System.currentTimeMillis

    println("Elapsed time: " + (t1 - t0) + "ms")

    result.foreach(println)

    sc.stop()
  }
}

object CassandraReadDataframe {

  def main(args: Array[String]) {

    val CassandraHost = "127.0.0.1"

    // Tell Spark the address of one Cassandra node:
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", CassandraHost)
      .set("spark.cleaner.ttl", "3600")
      .setMaster("local[4]")
      .setAppName("Lighthouse")

    // Connect to the Spark cluster:
    lazy val sc = new SparkContext(conf)

    val cc = new CassandraSQLContext(sc)

    cc.setKeyspace("highschool")
    val df = cc.cassandraSql("SELECT * FROM students")
    df.collect.foreach(println)

    sc.stop()
  }
}

object CrossdataReadDataframe {

  def main(args: Array[String]) {

    val CassandraHost = "127.0.0.1"

    // Tell Spark the address of one Cassandra node:
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", CassandraHost)
      .set("spark.cleaner.ttl", "3600")
      .setMaster("local[4]")
      .setAppName("Lighthouse")

    // Connect to the Spark cluster:
    lazy val sc = new SparkContext(conf)

    val cc = new CrossdataSQLContext(sc)

    val rdd = cc.sql("SELECT * FROM highschool.students")
    rdd.collect.foreach(println)

    sc.stop()
  }
}

object CassandraNative {
  def main(args: Array[String]) {

    val cluster: Cluster = Cluster.builder.addContactPoint("127.0.0.1").build

    val session: Session = cluster.connect

    val t0 = System.currentTimeMillis

    val result = session.execute("SELECT * FROM highschool.students")

    val t1 = System.currentTimeMillis

    println("Elapsed time: " + (t1 - t0) + "ms")

    println("Result of SELECT *:")

    import scala.collection.JavaConversions._

    result.all().foreach(println)

    cluster.close()
  }
}

object SparkVsNative {

  def executeQueryInSpark(sqlContext: SQLContext, table: String): Long = {
    // Once tables have been registered, you can run SQL queries over them.
    println("Result of SELECT *:")

    val query: String = "SELECT * FROM " + table

    val t0 = System.currentTimeMillis

    val result = sqlContext.sql(query).collect

    val t1 = System.currentTimeMillis

    val elapsedTime: Long = t1 - t0

    println("Elapsed time: " + elapsedTime + "ms")

    //result.foreach(println)

    elapsedTime
  }

  def executeTestInSpark(catalog: String, table: String): Long = {
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")

    val sc = new SparkContext("local[4]", "Lighthouse", conf)

    val sqlContext = new SQLContext(sc)

    sqlContext.sql(
      "CREATE TEMPORARY TABLE " + table + " USING org.apache.spark.sql.cassandra " +
        "OPTIONS (keyspace \"" + catalog + "\", table \"" + table + "\", cluster \"Test Cluster\", pushdown \"true\")"
        .stripMargin)

    var minTime: Long = Long.MaxValue

    for(a <- 0 to 100){
      val execTime: Long = executeQueryInSpark(sqlContext, table)
      if(execTime < minTime){
        minTime = execTime
      }
    }

    sc.stop()

    minTime
  }

  def executeTestInNative(catalog: String, table: String): Long = {
    val cluster: Cluster = Cluster.builder.addContactPoint("127.0.0.1").build

    val session: Session = cluster.connect

    val query: String = "SELECT * FROM " + catalog + "." + table

    val t0 = System.currentTimeMillis

    val result = session.execute(query)

    val t1 = System.currentTimeMillis

    val elapsedTime: Long = t1 - t0

    println("Elapsed time: " + elapsedTime + "ms")

    println("Result of SELECT *:")

    import scala.collection.JavaConversions._

    //result.all().foreach(println)

    cluster.close()

    elapsedTime
  }

  def executeTestInSparkCassandra(catalog: String, table: String): Long = {

    val CassandraHost = "127.0.0.1"

    // Tell Spark the address of one Cassandra node:
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", CassandraHost)
      .set("spark.cleaner.ttl", "3600")
      .setMaster("local[4]")
      .setAppName("Lighthouse")

    // Connect to the Spark cluster:
    lazy val sc = new SparkContext(conf)

    val cc = new CassandraSQLContext(sc)

    cc.setKeyspace(catalog)

    var minTime: Long = Long.MaxValue

    for(a <- 0 to 100){
      val execTime: Long = executeQueryInSpark(cc, table)
      if(execTime < minTime){
        minTime = execTime
      }
    }

    sc.stop()

    minTime
  }

  def main(args: Array[String]) {
    val catalog: String = "test"
    val table: String = "insurance"
    val nativeTime: Long = executeTestInNative(catalog, table)
    val sparkTime: Long = executeTestInSpark(catalog, table)
    val sparkCassandraTime: Long = executeTestInSparkCassandra(catalog, table)
    println("Native: " + nativeTime + "ms")
    println("Spark: " + sparkTime + "ms")
    println("SparkCassandra: " + sparkCassandraTime + "ms")
  }
}
