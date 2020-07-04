package tekcrux

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.commons.io.FileUtils
import java.io.File

object DataSourceHive {
  def main(args: Array[String]) {
    //System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    val warehouseLocation = "/home/cloudera/workspace/workspace-spark-sql/spark_sql_scala/spark-warehouse";

    val spark = SparkSession
      .builder()
      .appName("DataSourceHive")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.master", "local[1]")
      .enableHiveSupport()
      .getOrCreate()      
      
    import spark.implicits._
  
    spark.sparkContext.setLogLevel("ERROR")
    
    spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    spark.sql("LOAD DATA LOCAL INPATH 'resources/kv1.txt' INTO TABLE src")
     
    // Queries are expressed in HiveQL
    spark.sql("SELECT * FROM src").show()
    
    // Aggregation queries are also supported.
    spark.sql("SELECT COUNT(*) FROM src").show()
    
    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")
    sqlDF.show()
    
    
    spark.stop()
  }
}