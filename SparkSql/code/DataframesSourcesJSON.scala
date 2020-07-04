package tekcrux

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, IntegerType}
import org.apache.spark.sql.SaveMode

object DataframesSourcesJSON extends App {
  
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
  
  val spark = SparkSession
                .builder
                .master("local[2]")
                .appName("DataframesSourcesJSON")
                .getOrCreate()
      
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  
  val flightJsonPath = "./data/flight-data/json/2010-summary.json"
  val flightJsonOutPath = "./data/flight-data/json/out"
  
  val myFields = Array( 
               new StructField("DEST_COUNTRY_NAME", StringType, true),
               new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
               new StructField("count", IntegerType, false) )
               
  val mySchema = new StructType(myFields)
  
  // failfast -> exception; 
  val flightsDf = spark.read
                      .format("json")
                      .option("mode", "FAILFAST") 
                      .schema(mySchema)
                      .load(flightJsonPath)
                      
   //val flightsDf = spark.read.json(flightJsonPath)             
                      
   flightsDf.show(5)
   println( "count=" + flightsDf.count() )
        
   flightsDf.limit(50).write
                      .format("json")                      
                      .mode(SaveMode.Append)
                      .save(flightJsonOutPath)
            
  
}