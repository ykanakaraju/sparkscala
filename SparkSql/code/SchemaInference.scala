package tekcrux

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoder

object SchemaInference {
  
  case class Person(name: String, age: Long)
  
  def main(args: Array[String]) {
    //System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    val spark = SparkSession
      .builder.master("local[2]")
      .appName("SchemaInference")
      .getOrCreate()
      
    import spark.implicits._
    
    spark.sparkContext.setLogLevel("ERROR")
    
    val peopleDF = spark.sparkContext
      .textFile("people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    
    println("<---------- 1 ---------->")
    println(peopleDF.getClass)
    peopleDF.printSchema()
    
    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by Spark
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "Name: " + teenager(0) + ";   Age:" + teenager(1)).show()
    // or by field name
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
    
    spark.stop()
  }
}