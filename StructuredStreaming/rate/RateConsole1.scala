package rate

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RateConsole1 extends App {
  
    val spark = SparkSession
              .builder
              .master("local[2]")
              .appName("DataSourceBasic")
              .getOrCreate()
      
    spark.sparkContext.setLogLevel("ERROR")
    
    import spark.implicits._
        
    val df = spark
          .readStream
          .format("rate")
          .option("rowsPerSecond", 5)
          .load()
    
    // df will have two columns: timestamp: Timestamp, value: Int
    
    val resultDF = df.withColumn("newValue", col("value") * 10)
        
    val query = resultDF.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .start()

    query.awaitTermination()  
}

