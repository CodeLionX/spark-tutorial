package de.hpi.spark_tutorial

import de.hpi.spark_tutorial.schemas._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object Sindy extends App {

  val dataDir = "data/tpch"
  val datasetNames = Seq(
    Customer.name,
    Lineitem.name,
    Nation.name,
    Orders.name,
    Part.name,
    Region.name,
    Supplier.name
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
    val datasets: Seq[DataFrame] = inputs.map(name => {
      spark.read
//        .option("inferSchema", "true")
        .option("header", "true")
        .option("quote", "\"")
        .option("delimiter", ";")
        .csv(name)
    })

    // cells per file
    val columnValueTuplesPerFile: Seq[Dataset[(String, String)]] = datasets.map(df => {
      val cols = df.columns
      // explicitly convert each value to string
      df.map(_.toSeq.map(String.valueOf))
        .flatMap(row => {
          row.zip(cols)
        })
    })

    val columnValueTuples: Dataset[(String, String)] = columnValueTuplesPerFile.reduce{ (ds1, ds2) => ds1.union(ds2) }
    columnValueTuples.show()

    // attribute sets per file
    val attributeSets = columnValueTuples
      .map(tuple => tuple._1 -> Set(tuple._2))
        .groupByKey( _._1 )
        .mapValues(_._2)
        .reduceGroups( _ ++ _ )
        .map(_._2)
        .distinct()

    val inclusionLists = attributeSets.flatMap( attributeSet => {
        attributeSet.map( value => value -> (attributeSet - value) )
      })
    inclusionLists.show()
    val x = inclusionLists.collect().toMap
    println(x.get("N_NATIONKEY"))

    val inds = inclusionLists
      .groupByKey(_._1)
      .mapValues(_._2)
      .reduceGroups( (a, b) => a.intersect(b) )
      .filter(_._2.nonEmpty)

    val materializedInds = inds.collect()
    materializedInds.foreach{ case (key, dependends) =>
      println(s"$key < ${dependends.mkString(", ")}")
    }
  }
}
