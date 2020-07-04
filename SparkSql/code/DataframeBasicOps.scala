package tekcrux

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoder
import org.apache.commons.io.FileUtils;
import java.io.File;
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, IntegerType}

object DataframeBasicOps extends App {
  
  //System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
  val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("DataSourceBasic")
      .getOrCreate()
   
  spark.sparkContext.setLogLevel("ERROR")
 
  // ---------------------------------------------------
  // Example 1 - Basic Dataframe using programmatic data
  // ---------------------------------------------------
  val dfRange = spark.range(500).toDF("number")
  //val dfBasic = df.select(df.col("number"))
  val dfBasic = dfRange.select("number")
  dfBasic.printSchema()
  println( dfBasic.schema )
  println( dfBasic.columns.mkString )
  
  // ---------------------------------------------------
  // Example 2 - Basic Dataframe using json file
  // ---------------------------------------------------
  printSeparatorLine("Example 2")
  val df = spark.read
                .format("json")
                .load("./data/flight-data/json/2015-summary.json")
           
  println( df.schema )
  df.printSchema()
  
  // ---------------------------------------------------
  // Example 3 - Enforcing schema on a dataframe
  // ---------------------------------------------------
  printSeparatorLine("Example 3")
    
  import org.apache.spark.sql.types.Metadata
  
  val myManualSchema = StructType(
      Array(
        StructField("DEST_COUNTRY_NAME", StringType, true),
        StructField("ORIGIN_COUNTRY_NAME", StringType, true),
        StructField("count", IntegerType, false)
      )
   )
  
  val dfSchema = spark.read
                      .format("json")
                      .schema(myManualSchema)
                      .load("./data/flight-data/json/2015-summary.json")
  
  dfSchema.printSchema()
  
  // ----------------------------------------------------------
  // Example 4 - Explicitly creating dataframes as schema RDDs
  // ----------------------------------------------------------
  printSeparatorLine("Example 4")
  import org.apache.spark.sql.Row
    
  val mySchema = new StructType(Array(
    new StructField("name", StringType, true),
    new StructField("city", StringType, true),
    new StructField("age", IntegerType, false)))
  
  val myRows = Seq(Row("Raju", "Hyderabad", 40), Row("Ravi", "Chennai", 30))
  val myRDD = spark.sparkContext.parallelize(myRows)
  val myDf = spark.createDataFrame(myRDD, mySchema)
  myDf.show()  
   
  // ----------------------------------------------------------
  // Example 5 - Implicit Schema using toDF
  // ----------------------------------------------------------
  printSeparatorLine("Example 5")
  import spark.implicits._
  
  val mySeqDf = Seq(("Hello", 2, 1L)).toDF("col1", "col2", "col3")
  mySeqDf.printSchema()
    
  // ----------------------------------------------------------
  // Example 6 - select & selectExpr
  // ----------------------------------------------------------
  printSeparatorLine("Example 6")
  
  df.select("DEST_COUNTRY_NAME").show(2)
  df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)
  
  import org.apache.spark.sql.functions.{expr, col, column}
  df.select(
      df.col("DEST_COUNTRY_NAME"),
      col("DEST_COUNTRY_NAME"),
      column("DEST_COUNTRY_NAME"),
      'DEST_COUNTRY_NAME,
      $"DEST_COUNTRY_NAME",
      expr("DEST_COUNTRY_NAME"))
    .show(2)
    
  df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)
  
  df.select(expr("DEST_COUNTRY_NAME as destination")
    .alias("DEST_COUNTRY_NAME"))
    .show(2)
  
  // ----------------------------------------------------------
  // Example 7 - selectExpr
  // ---------------------------------------------------------- 
  printSeparatorLine("Example 7")
  df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)
  
  df.selectExpr(
    "*", // include all original columns
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
  .show(2)
  
  df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)
  
  // ----------------------------------------------------------
  // Example 8 - lit
  // ---------------------------------------------------------- 
  printSeparatorLine("Example 8")
  import org.apache.spark.sql.functions.lit
  df.select(expr("*"), lit(1).as("One")).show(2)
    
  // ----------------------------------------------------------
  // Example 9 - withColumn & withColumnRenamed
  // ---------------------------------------------------------- 
  printSeparatorLine("Example 9")
  
  df.withColumn("numberOne", lit(1)).show(2)
  
  df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
    .show(2)
  
  df.withColumn("Destination", expr("DEST_COUNTRY_NAME")).columns
  
  df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns
  
  // ----------------------------------------------------------
  // Example 10 - drop
  // ----------------------------------------------------------
  printSeparatorLine("Example 10")
  df.drop("ORIGIN_COUNTRY_NAME").columns
  df.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").columns
    
  // ----------------------------------------------------------
  // Example 11 - cast
  // ----------------------------------------------------------
  printSeparatorLine("Example 11")
  df.withColumn("count2", col("count").cast("long"))
  
  // ----------------------------------------------------------
  // Example 12 - filter & where
  // ----------------------------------------------------------
  printSeparatorLine("Example 12")
  
  df.filter(col("count") < 2).show(2)
  df.filter("count < 2").show(2)
  
  df.where(col("count") < 2).show(2)
  df.where("count < 2").show(2)
    
  // ----------------------------------------------------------
  // Example 13 - distinct
  // ----------------------------------------------------------
  printSeparatorLine("Example 13")
  
  df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
  df.select("ORIGIN_COUNTRY_NAME").distinct().count()
    
  // ----------------------------------------------------------
  // Example 14 - sample
  // ----------------------------------------------------------
  printSeparatorLine("Example 14")
  
  val seed = 5
  val withReplacement = false
  val fraction = 0.5
  df.sample(withReplacement, fraction, seed).count()
    
  // ----------------------------------------------------------
  // Example 15 - randomSplit
  // ----------------------------------------------------------
  printSeparatorLine("Example 15")
  
  val dataFrames = df.randomSplit(Array(0.25, 0.75), seed)
  dataFrames(0).count()
  dataFrames(1).count()  
  
  // ----------------------------------------------------------
  // Example 16 - union
  // ----------------------------------------------------------
  printSeparatorLine("Example 16")
  
  import org.apache.spark.sql.Row
  val schema = df.schema
  val newRows = Seq(
    Row("India", "France", 2L),
    Row("India", "Germany", 1L)
  )
  val parallelizedRows = spark.sparkContext.parallelize(newRows)
  val newDF = spark.createDataFrame(parallelizedRows, schema)
  df.union(newDF)
    .where("count = 1")
    .where($"ORIGIN_COUNTRY_NAME" =!= "United States")
    .show()
  
  // ----------------------------------------------------------
  // Example 17 - sort & orderBy
  // ----------------------------------------------------------
  printSeparatorLine("Example 17")
  
  df.sort("count").show(5)
  df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
  df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)
  
  import org.apache.spark.sql.functions.{desc, asc}
  df.orderBy(expr("count desc")).show(2)
  df.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(2)
  
  
  // ----------------------------------------------------------
  // Example 18 - limit
  // ----------------------------------------------------------
  printSeparatorLine("Example 18")
  df.limit(5).show()
  df.orderBy(expr("count desc")).limit(6).show()
    
  // ----------------------------------------------------------
  // Example 19 - repartition and coalesce
  // ----------------------------------------------------------
  printSeparatorLine("Example 19")
  
  df.rdd.getNumPartitions
  df.repartition(5)
  df.repartition(col("DEST_COUNTRY_NAME"))
  df.repartition(5, col("DEST_COUNTRY_NAME"))
  df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)
    
  // ----------------------------------------------------------
  // Example 20 - collecting data to the driver
  // ----------------------------------------------------------
  printSeparatorLine("Example 20")
  val collectDF = df.limit(10)
  collectDF.take(5) 
  collectDF.show() 
  collectDF.show(5, false)
  collectDF.collect()
    
  // ----------------------------------------------------------
  // Example 21 - explain
  // ----------------------------------------------------------
  printSeparatorLine("Example 21")
  
  df.explain()
  
  // =================================================
  def printSeparatorLine(str: String) {
    println()
    println( "-" * str.length())
    println( str )
    println( "-" * str.length())
  }
  
}