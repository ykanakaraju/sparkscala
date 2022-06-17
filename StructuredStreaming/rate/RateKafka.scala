package rate

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object RateKafka extends App {
  
    val spark = SparkSession
              .builder
              .master("local[2]")
              .appName("DataSourceBasic")
              .getOrCreate()
      
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.streaming.checkpointLocation", "/home/kanak/scalaspark/checkpoint/kafka");
    
    import spark.implicits._
        
    val df = spark.readStream
          .format("rate")
          .option("rowsPerSecond", 5)
          .load()
    
    val resultDF = df.withColumn("value", concat(lit("Message: "), col("value")))
                     .selectExpr("CAST(timestamp AS STRING)", "CAST(value AS STRING)")
                     .withColumnRenamed("timestamp", "key")
        
    val query = resultDF.writeStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("topic", "t1")
          .start()

    query.awaitTermination()  
}

