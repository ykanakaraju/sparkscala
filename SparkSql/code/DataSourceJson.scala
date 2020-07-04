package tekcrux

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.functions._

import org.apache.commons.io.FileUtils;
import java.io.File;

object DataSourceJson {
  def main(args: Array[String]) {
    //System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    val spark = SparkSession
      .builder.master("local[2]")
      .appName("DataSourceJson")
      .getOrCreate()
      
    import spark.implicits._
    
    val path = "./resources/people.json"
    val peopleDF = spark.read.json(path)

    peopleDF.printSchema
    peopleDF.show
    
    peopleDF.createOrReplaceTempView("people")

    val teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
    teenagerNamesDF.show
    
    // Alternatively, a DataFrame can be created for a JSON dataset 
    // represented by an RDD[String] storing one JSON object per string
    val otherPeopleRDD = spark.sparkContext.makeRDD(
      """{"age":"43", "name":"Kanakaraju","address":{"city":"Hyderabad","state":"Telangana"}}""" :: Nil)
    val otherPeople = spark.read.json(otherPeopleRDD)
    otherPeople.printSchema
    otherPeople.show
    
    spark.stop()
  }
}