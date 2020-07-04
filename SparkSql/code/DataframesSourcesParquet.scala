package tekcrux

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object DataframeSourcesParquet extends App {
  
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    val spark = SparkSession
                  .builder
                  .master("local[2]")
                  .appName("DataframesSourcesJSON")
                  .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    
    val parquetPath = "./data/flight-data/parquet/2010-summary.parquet"
    val parquetOutPath = "./data/flight-data/parquet/out/"
    
    val dfFlights = spark.read.format("parquet").load(parquetOutPath)
    dfFlights.show(10)
    
    val dfNonUsa = dfFlights.where(expr("DEST_COUNTRY_NAME") =!= "United States")
    dfNonUsa.show(5)
    
    //dfNonUsa.write.format("parquet").mode("append").save(parquetOutPath)
}