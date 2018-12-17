package de.hpi.spark_tutorial

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.backuity.clist
import org.backuity.clist.opt


object Sindy extends clist.CliMain[Unit](
      name = "Sindy",
      description = "discover inclusion dependencies with spark"
    ) {

  var path: String  = opt[String](description = "path to the folder containing the CSV data", default = "./TPCH")
  var cores: Int = opt[Int](description = "number of cores", default = 4)

  override def run: Unit = {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val dataDir = this.path
    val datasetNames = Seq("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
    val inputs = datasetNames.map(name => s"$dataDir/tpch_$name.csv")

    // Create a SparkSession to work with Spark
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Sindy")
      .master(s"local[$cores]") // local, with <cores> worker cores
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
    val columnValueTuplesPerFile: Seq[Dataset[(String, String)]] = datasets.map(df => {
      val cols = df.columns // we do not need a broadcast-var as we use it only once
      df.map    ( row => row.toSeq.map(String.valueOf) ) // explicitly convert each value to string to help compiler (typer-phase)
        .flatMap( row => row.zip(cols)                 ) // combine values with their column names (create cells)
    })

    // combine tables: Dataset( value -> columnName ) with content from all tables
    val columnValueTuples: Dataset[(String, String)] = columnValueTuplesPerFile.reduce { (acc, ds2) => acc.union(ds2) }
    //  columnValueTuples.show()

    // attribute sets: Dataset( Set(columnName1, columnName2, ....)
    val attributeSets = columnValueTuples
      .map(tuple => tuple._1 -> Set(tuple._2)) // mixed value,Set(columnName) records
      .groupByKey(tuple => tuple._1) // group by value: key(value) -> (value, Set(columnName))
      .mapValues { case (_, columnSet) => columnSet } // map values to: key(value) -> Set(columnName)
      .reduceGroups((acc, columnSet) => acc ++ columnSet) // accumulate sets: value -> Set(columnName1, columnName2, ...)
      .map { case (_, columnSet) => columnSet } // throw away key: Set(columnName1, columnName2, ...)
      .distinct() // delete duplicates
    //  attributeSets.show()

    // inclusion lists: Dataset( columnName -> Set(otherColumnNames...)
    val inclusionLists = attributeSets.flatMap(attributeSet => {
      // map each value in set to the tuple: (the value, the set itself without the value)
      attributeSet.map(value => value -> (attributeSet - value))
    })
    //  inclusionLists.show()

    // inds: Dataset( columnName -> Set(otherColumnNames...)
    val inds = inclusionLists
      .groupByKey(tuple => tuple._1) // group by single column: key(column) -> (column, Set(columnName))
      .mapValues { case (_, otherColumnSet) => otherColumnSet } // map values to: key(column) -> Set(columnNames...)
      .reduceGroups((acc, columnSet) => acc.intersect(columnSet)) // build set intersections per key: column -> Set(columnNames...)
      .filter(tuple => tuple._2.nonEmpty) // remove records with empty sets: column -> Set(columnNames...)

    // collect results and print to stdout
    inds.collect()
      .sortBy(tuple => tuple._1)
      .foreach { case (dependent, references) => println(s"$dependent < ${references.mkString(", ")}") }
  }
}
