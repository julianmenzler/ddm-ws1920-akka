package de.hpi.spark_tutorial

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.{collect_set, lit, col}


object Sindy {
  val columnName = "attribute"

  type AttributeSet = Seq[String]
  type Attribute = String

  case class Inclusion(attribute: Attribute,
                       attributeSet: AttributeSet)

  case class InclusionList(attribute: Attribute,
                           attributeSets: Seq[AttributeSet])

  case class Value(value: String,
                   attribute: Attribute)

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    // TODO: Look at partitioning

    // Read csv data into Dataframes
    // List[String] => List[DataFrame]
    val dataframes = inputs.map(input => {
      spark.read
        .option("header", "true")
        .option("quote", "\"")
        .option("delimiter", ";")
        .csv(input)
    })

    import spark.implicits._

    // TODO: Add partinoniong here: p(v) = hash(v) mod n

    // Cells contains a dataframe of ALL distinct values per column + attribute name
    val cells = dataframes.map(df => {
      // Get the columns names and use them to select respective columns from the dataframe
      val distinctColumnValues = df.columns
        .map(col => df.select(df(col)).distinct().withColumn(columnName, lit(col)))
      // Merge the distinct (column value, column name)-dataframes
      distinctColumnValues.reduce((df1, df2) => df1.union(df2))
    }).reduce((df1, df2) => df1.union(df2))

    // Attribute sets are grouping together attributes that have the same value in the column
    val groupedAttributes = cells
      .groupBy(cells.columns(0))
      .agg(collect_set(cells.columns(1)))

    val attributeSets = groupedAttributes.select(groupedAttributes.columns(1)).as[AttributeSet]

    // Build inclusion list from attribute sets
    val inclusionLists = attributeSets.flatMap(inclusion => {
        // Generate set e.g [a,b] => [a, [b]], [b, [a]]
        inclusion.map(attribute => (attribute, inclusion.filter(_ != attribute)))
      }).toDF("attribute", "attributeSet")
        .as[Inclusion];

    // Build result list by collecting attribute sets and
    // merging them using the intersection
    val results = inclusionLists
      .groupBy(inclusionLists.columns(0))
      .agg(collect_set(inclusionLists.columns(1)))
      .toDF("attribute", "attributeSets")
      .as[InclusionList]
      .map(inclusionList => (inclusionList.attribute, inclusionList.attributeSets.reduce((a,b) => a.intersect(b))))
      .toDF("attribute", "attributeSet")
      .as[Inclusion]
      .filter(inclusionList => inclusionList.attributeSet.nonEmpty)
      .orderBy(col("attribute").desc)

    // TODO: Ordering in spark does not work
    var r = results.coll


    // Print results in desired format
    results.foreach(result => println(s"${result.attribute} < ${result.attributeSet.mkString(", ")}"))
  }
}

