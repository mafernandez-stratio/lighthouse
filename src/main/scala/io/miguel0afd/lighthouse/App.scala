package io.miguel0afd.lighthouse

import com.datastax.driver.core.{Row, Session, Cluster}
import org.apache.spark.sql.SQLContext
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

object CassandraReadDataframe {

  def executeQuery(sqlContext: SQLContext): Unit = {
    // Once tables have been registered, you can run SQL queries over them.
    println("Result of SELECT *:")

    val t0 = System.currentTimeMillis

    val result = sqlContext.sql("SELECT * FROM students").collect

    val t1 = System.currentTimeMillis

    println("Elapsed time: " + (t1 - t0) + "ms")

    result.foreach(println)
  }

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

    for(a <- 0 to 100){
      executeQuery(sqlContext)
    }

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
