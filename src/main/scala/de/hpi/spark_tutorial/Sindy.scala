package de.hpi.spark_tutorial

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object Sindy extends App {

  val dataDir = "data/tpch"
  val datasetNames = Seq("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
  val inputs = datasetNames.map( name => s"$dataDir/tpch_$name.csv" )

  // Create a SparkSession to work with Spark
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Sindy")
    .master("local[4]") // local, with 4 worker cores
    .getOrCreate()

  // configure
  spark.conf.set("spark.sql.shuffle.partitions", "16")
  import spark.implicits._

  // read files
  val datasets: Seq[DataFrame] = inputs.map(name => {
    spark.read
//      .option("inferSchema", "true") // ignore data types (we want to have strings)
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(name)
  })

  // cells per file: for each table a Dataset( value -> columnName )
  val columnValueTuplesPerFile: Seq[Dataset[(String, String)]] = datasets.map(df => df
    .map    ( row => row.toSeq.map(String.valueOf) ) // explicitly convert each value to string to help compiler (typer-phase)
    .flatMap( row => row.zip(df.columns)           ) // combine values with their column names (create cells)
  )

  // combine tables: Dataset( value -> columnName ) with content from all tables
  val columnValueTuples: Dataset[(String, String)] = columnValueTuplesPerFile.reduce{ (acc, ds2) => acc.union(ds2) }
//  columnValueTuples.show()

  // attribute sets: Dataset( Set(columnName1, columnName2, ....)
  val attributeSets = columnValueTuples
    .map         ( tuple => tuple._1 -> Set(tuple._2)   ) // mixed value,Set(columnName) records
    .groupByKey  ( tuple => tuple._1                    ) // group by value: key(value) -> (value, Set(columnName))
    .mapValues   { case (_, columnSet) => columnSet     } // map values to: key(value) -> Set(columnName)
    .reduceGroups( (acc, columnSet) => acc ++ columnSet ) // accumulate sets: value -> Set(columnName1, columnName2, ...)
    .map         { case (_, columnSet) => columnSet     } // throw away key: Set(columnName1, columnName2, ...)
    .distinct()                                           // delete duplicates
//  attributeSets.show()

  // inclusion lists: Dataset( columnName -> Set(otherColumnNames...)
  val inclusionLists = attributeSets.flatMap( attributeSet => {
      // map each value in set to the tuple: (the value, the set itself without the value)
      attributeSet.map( value => value -> (attributeSet - value) )
    })
//  inclusionLists.show()

  // inds: Dataset( columnName -> Set(otherColumnNames...)
  val inds = inclusionLists
    .groupByKey  ( tuple => tuple._1                            ) // group by single column: key(column) -> (column, Set(columnName))
    .mapValues   { case (_, otherColumnSet) => otherColumnSet   } // map values to: key(column) -> Set(columnNames...)
    .reduceGroups( (acc, columnSet) => acc.intersect(columnSet) ) // build set intersections per key: column -> Set(columnNames...)
    .filter      ( tuple => tuple._2.nonEmpty                   ) // remove records with empty sets: column -> Set(columnNames...)

  // collect results and print to stdout
  inds.collect()
    .sortBy     ( tuple => tuple._1 )
    .foreach    { case (dependent, references) => println(s"$dependent < ${references.mkString(", ")}") }
}
