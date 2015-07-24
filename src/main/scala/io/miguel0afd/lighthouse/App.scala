package io.miguel0afd.lighthouse

import com.datastax.driver.core.{Row, Session, Cluster}
import com.stratio.crossdata.fuse.CrossdataSQLContext
import io.miguel0afd.lighthouse.Calculation.Calculation
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkContext, SparkConf}

// One method for defining the schema of an RDD is to make a case class with the desired column
// names and types.
case class Record(key: Int, value: String)

object DefaultConstants {
  val nIterations = 100
  val cluster = "Test Cluster"
  val catalog = "bug"
  val table = "companies"
  val secondTable = "counties"
  val firstJoinField: Any = "cif"
  val secondJoinField: Any = "id"
}

object Calculation extends Enumeration {
  type Calculation = Value
  val Min, Mean = Value
}

object MapDataframe {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Lighthouse").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext.implicits._

    val df = sc.parallelize((1 to DefaultConstants.nIterations).map(i => Record(i, s"val_$i"))).toDF()
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

    val rdd = sc.cassandraTable(DefaultConstants.catalog, DefaultConstants.table)
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
      "CREATE TEMPORARY TABLE " + DefaultConstants.table + " USING org.apache.spark.sql.cassandra OPTIONS " +
        "(keyspace \"" + DefaultConstants.catalog + "\", table \"" + DefaultConstants.table + "\", " +
        "cluster \"" + DefaultConstants.cluster + "\", pushdown \"true\")".stripMargin)

    // Once tables have been registered, you can run SQL queries over them.
    println("Result of SELECT *:")

    val t0 = System.currentTimeMillis

    val result = sqlContext.sql("SELECT * FROM " + DefaultConstants.table).collect

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

    cc.setKeyspace(DefaultConstants.catalog)
    val df = cc.cassandraSql("SELECT * FROM " + DefaultConstants.table)
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

    val cc = new SQLContext(sc)

    val rdd = cc.sql("SELECT * FROM " + DefaultConstants.catalog + "." + DefaultConstants.table)
    rdd.collect.foreach(println)

    sc.stop()
  }
}

object CassandraNative {
  def main(args: Array[String]) {

    val cluster: Cluster = Cluster.builder.addContactPoint("127.0.0.1").build

    val session: Session = cluster.connect

    val t0 = System.currentTimeMillis

    val result = session.execute("SELECT * FROM " + DefaultConstants.catalog + "." + DefaultConstants.table)

    val t1 = System.currentTimeMillis

    println("Elapsed time: " + (t1 - t0) + "ms")

    println("Result of SELECT *:")

    import scala.collection.JavaConversions._

    result.all().foreach(println)

    cluster.close()
  }
}

object SparkVsNative {

  def executeQueryInSpark(df: DataFrame, table: String): Long = {
    // Once tables have been registered, you can run SQL queries over them.
    println("Result of SELECT *:")

    val t0 = System.currentTimeMillis

    val result = df.collect

    val t1 = System.currentTimeMillis

    val elapsedTime: Long = t1 - t0

    println("Elapsed time: " + elapsedTime + "ms")

    //result.foreach(println)

    elapsedTime
  }

  def executeTestInSpark(catalog: String, table: String, calculation: Calculation, persist: Boolean = false): Long = {
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")

    val sc = new SparkContext("local[4]", "Lighthouse", conf)

    val sqlContext = new SQLContext(sc)

    sqlContext.sql(
      "CREATE TEMPORARY TABLE " + table + " USING org.apache.spark.sql.cassandra OPTIONS " +
        "(keyspace \"" + catalog + "\", table \"" + table + "\", " +
        "cluster \"" + DefaultConstants.cluster + ("\", pushdown \"true\")").stripMargin)

    val query: String = "SELECT * FROM " + table

    val df = sqlContext.sql(query)
    if(persist) df.persist()

    var calculatedTime: Long = 0
    if(calculation.equals(Calculation.Min)){
      var minTime: Long = Long.MaxValue
      for(a <- 0 to DefaultConstants.nIterations){
        val execTime: Long = executeQueryInSpark(df, table)
        if(execTime < minTime){
          minTime = execTime
        }
      }
      calculatedTime = minTime
    } else {
      var accTime: Long = 0
      for(a <- 0 to DefaultConstants.nIterations){
        val execTime: Long = executeQueryInSpark(df, table)
        accTime += execTime
      }
      calculatedTime = accTime / DefaultConstants.nIterations
    }

    if(persist) df.unpersist()
    sc.stop()

    calculatedTime
  }

  def executeQueryInNative(session: Session, query: String): Long = {

    println("Result of SELECT *:")

    val t0 = System.currentTimeMillis

    val result = session.execute(query).all()

    val t1 = System.currentTimeMillis

    val elapsedTime: Long = t1 - t0

    println("Elapsed time: " + elapsedTime + "ms")

    //result.foreach(println)

    elapsedTime
  }

  def executeTestInNative(catalog: String, table: String, calculation: Calculation): Long = {
    val cluster: Cluster = Cluster.builder.addContactPoint("127.0.0.1").build

    val session: Session = cluster.connect

    val query: String = "SELECT * FROM " + catalog + "." + table

    var calculatedTime: Long = 0
    if(calculation.equals(Calculation.Min)){
      var minTime: Long = Long.MaxValue
      for(a <- 0 to DefaultConstants.nIterations){
        val execTime: Long = executeQueryInNative(session, query)
        if(execTime < minTime){
          minTime = execTime
        }
      }
      calculatedTime = minTime
    } else {
      var accTime: Long = 0
      for(a <- 0 to DefaultConstants.nIterations){
        val execTime: Long = executeQueryInNative(session, query)
        accTime += execTime
      }
      calculatedTime = accTime / DefaultConstants.nIterations
    }

    cluster.close()

    calculatedTime
  }

  def executeTestInSparkCassandra(catalog: String, table: String, calculation: Calculation, persist: Boolean = false):
    Long = {

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

    val query: String = "SELECT * FROM " + table

    val df = cc.cassandraSql(query)
    if(persist) df.persist()

    var calculatedTime: Long = 0
    if(calculation.equals(Calculation.Min)){
      var minTime: Long = Long.MaxValue
      for(a <- 0 to DefaultConstants.nIterations){
        val execTime: Long = executeQueryInSpark(df, table)
        if(execTime < minTime){
          minTime = execTime
        }
      }
      calculatedTime = minTime
    } else {
      var accTime: Long = 0
      for(a <- 0 to DefaultConstants.nIterations){
        val execTime: Long = executeQueryInSpark(df, table)
        accTime += execTime
      }
      calculatedTime = accTime / DefaultConstants.nIterations
    }

    if(persist) df.unpersist()
    sc.stop()

    calculatedTime
  }

  def main(args: Array[String]) {

    val nativeMin: Long = executeTestInNative(DefaultConstants.catalog, DefaultConstants.table, Calculation.Min)
    val sparkMin: Long = executeTestInSpark(DefaultConstants.catalog, DefaultConstants.table, Calculation.Min)
    val sparkCassMin: Long = executeTestInSparkCassandra(DefaultConstants.catalog, DefaultConstants.table,
      Calculation.Min)

    val nativeMinWithPersist = executeTestInNative(DefaultConstants.catalog, DefaultConstants.table, Calculation.Min)
    val sparkMinWithPersist = executeTestInSpark(DefaultConstants.catalog, DefaultConstants.table, Calculation.Min,
      true)
    val sparkCassMinWithPersist = executeTestInSparkCassandra(DefaultConstants.catalog, DefaultConstants.table,
      Calculation.Min, true)

    val nativeMean = executeTestInNative(DefaultConstants.catalog, DefaultConstants.table, Calculation.Mean)
    val sparkMean = executeTestInSpark(DefaultConstants.catalog, DefaultConstants.table, Calculation.Mean)
    val sparkCassMean = executeTestInSparkCassandra(DefaultConstants.catalog, DefaultConstants.table,
      Calculation.Mean)

    val nativeMeanWithPersist = executeTestInNative(DefaultConstants.catalog, DefaultConstants.table, Calculation.Mean)
    val sparkMeanWithPersist = executeTestInSpark(DefaultConstants.catalog, DefaultConstants.table, Calculation.Mean,
      true)
    val sparkCassMeanWithPersist = executeTestInSparkCassandra(DefaultConstants.catalog, DefaultConstants.table,
      Calculation.Mean, true)

    println(" === MIN after " + DefaultConstants.nIterations + " iterations === ")
    println("Native: " + nativeMin + "ms")
    println("Spark: " + sparkMin + "ms")
    println("SparkCassandra: " + sparkCassMin + "ms")

    println(" === MIN after " + DefaultConstants.nIterations + " iterations & persist === ")
    println("Native: " + nativeMinWithPersist + "ms")
    println("Spark: " + sparkMinWithPersist + "ms")
    println("SparkCassandra: " + sparkCassMinWithPersist + "ms")

    println(" === MEAN after " + DefaultConstants.nIterations + " iterations === ")
    println("Native: " + nativeMean + "ms")
    println("Spark: " + sparkMean + "ms")
    println("SparkCassandra: " + sparkCassMean + "ms")

    println(" === MEAN after " + DefaultConstants.nIterations + " iterations & persist === ")
    println("Native: " + nativeMeanWithPersist + "ms")
    println("Spark: " + sparkMeanWithPersist + "ms")
    println("SparkCassandra: " + sparkCassMeanWithPersist + "ms")
  }

}

object CassandraJoin {

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

    cc.setKeyspace(DefaultConstants.catalog)
    val df = cc.cassandraSql("SELECT * FROM " + DefaultConstants.table + " JOIN " + DefaultConstants.secondTable +
      " ON " + DefaultConstants.firstJoinField + "=" + DefaultConstants.secondJoinField)

    val millis = System.currentTimeMillis()

    df.collect.foreach(println)

    println((System.currentTimeMillis-millis) + " Milliseconds")

    sc.stop()
  }
}


