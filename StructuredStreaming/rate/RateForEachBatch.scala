package rate

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

object RateForEachBatch extends App {
  
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
    
    val resultDF = df.withColumn("value", concat(lit("Message: "), col("value"))) 
             .selectExpr("CAST(timestamp AS STRING)", "CAST(value AS STRING)") 
             .withColumnRenamed("timestamp", "ts")
             .withColumnRenamed("value", "message")       
                
    val query = resultDF
        .writeStream
        .foreachBatch( 
            (batchDF: DataFrame, batchId: Long) => { 
              
                // Write to MySQL
                batchDF.write
                  .format("jdbc")
                  .option("url", "jdbc:mysql://localhost:3306/sparkdb")
                  .option("driver", "com.mysql.jdbc.Driver")
                  .option("dbtable", "rate_stream_1")
                  .option("user", "root")
                  .option("password", "kanakaraju")
                  .mode(SaveMode.Append)
                  .save()
                  
                // Write to CSV
                batchDF.write
                  .format("csv")
                  .option("header", "true")
                  .mode(SaveMode.Append)
                  .save("/home/kanak/scalaspark/output/csv")
                  
                // Write to Kafka
                batchDF.withColumnRenamed("ts", "key")
                  .withColumnRenamed("message", "value")
                  .write 
                  .format("kafka")
                  .option("kafka.bootstrap.servers", "localhost:9092")
                  .option("topic", "t1")
                  .save()
            }
         )
        .start()
        
    query.awaitTermination()  
}

