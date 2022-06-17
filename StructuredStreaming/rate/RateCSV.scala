package rate

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object RateCSV extends App {
  
    val spark = SparkSession
              .builder
              .master("local[2]")
              .appName("DataSourceBasic")
              .getOrCreate()
      
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.streaming.checkpointLocation", "/home/kanak/scalaspark/checkpoint/csv_sink");
    
    import spark.implicits._
        
    val df = spark
          .readStream
          .format("rate")
          .option("rowsPerSecond", 5)
          .load()
    
    val resultDF = df.withColumn("newValue", col("value") * 10)
        
    val query = resultDF
          .writeStream
          .outputMode("append")
          .trigger(Trigger.ProcessingTime("4 seconds"))
          .format("csv")
          .option("path", "/home/kanak/scalaspark/output/csv")
          .start()

    query.awaitTermination()  
}

