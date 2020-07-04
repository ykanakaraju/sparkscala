package tekcrux

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoder
import org.apache.commons.io.FileUtils;
import java.io.File;
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
   
object Ex1 extends App {
  
  val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("DataSourceBasic")
      .getOrCreate()
   
  spark.sparkContext.setLogLevel("ERROR")
  
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
   
   import spark.implicits._
    
   val ratingsCsvPath = "data/movielens_out_parquet/*.parquet"
   val out_path = "data/movielens_out_orc"
  
   val ratingsDf = spark.read.format("parquet").load(ratingsCsvPath)
                       
   ratingsDf.show()  
   
   /*ratingsDf.write
            .format("orc")                      
            .mode(SaveMode.Append)
            .save(out_path)*/
   
   

}




