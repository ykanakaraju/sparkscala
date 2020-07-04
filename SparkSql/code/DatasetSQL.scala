package tekcrux

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

case class Record2(key: Int, value: String)

object DatasetSQL {
  def main(args: Array[String]) {
    
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    val spark = SparkSession
      .builder.master("local[2]")
      .appName("DatasetSQL")
      .config("spark.master", "local")
      .getOrCreate()
      
    spark.sparkContext.setLogLevel("ERROR")
      
    import spark.implicits._
    
    // Snippet# 1 - Creating TmpView from a Dataset of Record objects
    // Encoders are created for Case Classes
    val df = spark.createDataFrame((1 to 20).map(i => Record2(i, s"value_$i")))    //S - String Interpolator
    println("<---------- 1 ---------->")
    println(df.getClass)
    df.printSchema()
    df.createOrReplaceTempView("records")
    
    // Running SQL directly on a Dataset
    spark.sql("SELECT * FROM records WHERE key > 5 AND key < 10").collect().foreach(println)
    val count = spark.sql("SELECT COUNT(*) FROM records").collect().head.getLong(0)
    val count1 = spark.sql("SELECT COUNT(*) FROM records").collect().foreach(println)
    println(s"COUNT(*): $count")
    
    val rddFromSql = spark.sql("SELECT key, value FROM records WHERE key < 10")
    rddFromSql.rdd.map(row => s"Key: ${row(0)}, Value: ${row(1)}").collect().foreach(println)

    df.where($"key" === 1).orderBy($"value".asc).select($"key").collect().foreach(println)
    df.orderBy($"key".desc).select($"key",$"value").collect().foreach(print)

    spark.stop()      
  }
}