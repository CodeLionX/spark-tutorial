package de.hpi.spark_tutorial

import de.hpi.spark_tutorial.schemas._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object Sindy extends App {

  val dataDir = "data/tpch"
  val datasetNames = Seq(
//    Customer.name,
//    Lineitem.name,
    Nation.name,
//    Orders.name,
//    Part.name,
    Region.name//,
//    Supplier.name
  )
  val inputs = datasetNames.map( name => s"$dataDir/$name.csv" )

  // Create a SparkSession to work with Spark
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Sindy")
    .master("local[4]") // local, with 4 worker cores
    .getOrCreate()

  // configure
  spark.conf.set("spark.sql.shuffle.partitions", "8")
  import spark.implicits._

  discoverINDs(inputs, spark)

  def discoverINDs(inputs: Seq[String], spark: SparkSession): Unit = {

    // read files
    val dataframes: Seq[DataFrame] = inputs.map(name => {
      spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(name)
    })

    implicit val mapEncoder: org.apache.spark.sql.Encoder[Map[Any, String]] =
      org.apache.spark.sql.Encoders.kryo[Map[Any, String]]
    implicit val mapEncoder2: org.apache.spark.sql.Encoder[Map[String, Any]] =
      org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    implicit val mapEncoder3: org.apache.spark.sql.Encoder[Any] =
      org.apache.spark.sql.Encoders.kryo[Any]

    dataframes.foreach(
      _.flatMap( row => {
        row.schema.fieldNames.map(name => {
          row.get(row.schema.fieldNames.indexOf(name)) -> name
        })
      })
        .show(5)
    )
    // TODO
  }
}
