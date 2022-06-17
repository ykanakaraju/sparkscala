package kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object KafkaConsole extends App {
  
    val spark = SparkSession
        .builder.master("local[2]")
        .appName("DataSourceBasic")
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")            
     
    val df = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "t1")
            .load()     
            
    /*
    df.printSchema()
    
    root
    |-- key: binary (nullable = true)
    |-- value: binary (nullable = true)
    |-- topic: string (nullable = true)
    |-- partition: integer (nullable = true)
    |-- offset: long (nullable = true)
    |-- timestamp: timestamp (nullable = true)
    |-- timestampType: integer (nullable = true)
    */              
          
    val df2 = df.selectExpr("CAST(key AS STRING)", 
                            "CAST(value AS STRING)",
                            "partition", 
                            "offset",
                            "topic",
                            "timestamp",
                            "timestampType")
    
    val query = df2.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()  
}

