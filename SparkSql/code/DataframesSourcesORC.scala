package tekcrux

import org.apache.spark.sql.SparkSession

object DataframesSourcesORC extends App {
  
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    val spark = SparkSession
                  .builder
                  .master("local[2]")
                  .appName("DataframesSourcesJSON")
                  .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    
    val orcPath = "./data/flight-data/orc/2010-summary.orc"
    val orcOutPath = "./data/flight-data/orc/out/"
    
    val dfFlights = spark.read.format("orc").load(orcPath)
    dfFlights.show(5)
    
    val dfNonUsa = dfFlights.where($"DEST_COUNTRY_NAME" =!= "United States")
    dfNonUsa.show(5)
    
    dfNonUsa.write.format("orc").mode("append").save(orcOutPath)
    
    spark.read.format("orc")
              .load(orcOutPath)
              .show(5)    
    
}