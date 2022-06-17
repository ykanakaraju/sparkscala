package socket

import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object SocketJSON1 extends App {
  
  val spark = SparkSession
        .builder.master("local[2]")
        .appName("DataSourceBasic")
        .getOrCreate()
      
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.streaming.checkpointLocation", "/home/kanak/scalaspark/checkpoint/csv_sink");
    
    import spark.implicits._
    
    val host = "localhost"
    val port = 9999
        
    val lines = spark.readStream
          .format("socket")
          .option("host", host)
          .option("port", port)
          .option("includeTimestamp", true)
          .load()
    
    val words = lines.as[(String, Timestamp)]
                  .flatMap(
                      line => line._1.split(" ").map(word => (word, line._2))
                   ).toDF("word", "timestamp")
    
    val query = words
          .writeStream
          .outputMode("append")
          .trigger(Trigger.ProcessingTime("4 seconds"))
          .format("json")
          .option("path", "/home/kanak/scalaspark/output/json")
          .start()

    query.awaitTermination() 
}




