package tekcrux

import org.apache.spark.sql.SparkSession

object DataframeRDDs extends App {
  
   System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
  val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("DataframeRDDs")
      .getOrCreate()
   
  val sc = spark.sparkContext
  
  sc.setLogLevel("ERROR")
  // ----------------------------------------
  // Example 1 - Dataset to RDD
  // ----------------------------------------
  printSeparatorLine("Example 1")
  val ds1 = spark.range(10)
  val rdd1 = ds1.rdd
  rdd1.take(3).foreach( println )
  
  // ----------------------------------------
  // Example 1 - DataFrame to RDD
  // ----------------------------------------
  printSeparatorLine("Example 2")
  val df1 = spark.range(10).toDF()
  val rdd2Row = df1.rdd
  val rdd2Long = rdd2Row.map(rowObject => rowObject.getLong(0))
  rdd2Long.take(3).foreach( println )
    
  
  // =================================================
  def printSeparatorLine(str: String) {
    println()
    println( "-" * str.length())
    println( str )
    println( "-" * str.length())
  }
  
  
  
}