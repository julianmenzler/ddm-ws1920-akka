package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.log4j.Logger
import org.apache.log4j.Level


object SimpleSpark extends App {

  override def main(args: Array[String]): Unit = {
    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    type OptionMap = Map[Symbol, Any]

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      list match {
        case Nil => map
        case "--path" :: value :: tail =>
          nextOption(map ++ Map('path -> value), tail)
        case "--cores" :: value :: tail =>
          nextOption(map ++ Map('cores -> value.toInt), tail)
        case option :: tail =>
          println("Unknown option "+option);
          System.exit(1)
          Map()
      }
    }
    val options = nextOption(Map(), args.toList)

    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("Sindy")
      .master(s"local[${options.getOrElse('cores, 4) }]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "8") //

    //------------------------------------------------------------------------------------------------------------------
    // Inclusion Dependency Discovery (Homework)
    //------------------------------------------------------------------------------------------------------------------

    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block
      val t1 = System.currentTimeMillis()
      println(s"Execution: ${t1 - t0} ms")
      result
    }

    val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
      .map(name => s"${options.getOrElse('path, "./TPCH")}/tpch_$name.csv")

    time { Sindy.discoverINDs(inputs, spark) }
  }
}
