package tekcrux

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr}

object DataframesDatesTimes extends App {
  
    val spark = SparkSession
          .builder.master("local[2]")
          .appName("DataSourceBasic")
          .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    
    import spark.implicits._
            
    import org.apache.spark.sql.functions.{current_date, current_timestamp}
    import org.apache.spark.sql.functions.{expr, col, lit}
  
    val dateDF = spark.range(10)
      .withColumn("today", current_date())
      .withColumn("now", current_timestamp())
    
    //dateDF.createOrReplaceTempView("dateTable")
    dateDF.printSchema()
    dateDF.show(false)
    
    import org.apache.spark.sql.functions.{date_add, date_sub}
    dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)
    
    
    import org.apache.spark.sql.functions.{datediff, months_between, to_date}
    
    dateDF.withColumn("week_ago", date_sub(col("today"), 7))
      .select(datediff(col("week_ago"), col("today"))).show(1)
      
    dateDF.select(to_date(lit("2017-20-11")),to_date(lit("2017-12-11"))).show(1)  
   
    dateDF.select(
        to_date(lit("2016-01-01")).alias("start"),
        to_date(lit("2017-05-22")).alias("end"))
      .select(months_between(col("start"), col("end"))).show(1)      
           
    spark.range(5).withColumn("date", lit("2017-01-01"))
        .select(to_date(col("date"))).show(1)  
    
    // date formatting    
    import org.apache.spark.sql.functions.{regexp_replace}
    val df1 = Seq( ("X", "20180101"), ("Y", "20180406") ).toDF("c1", "c2")
    val df2 = df1.withColumn(
            "c2", to_date(regexp_replace($"c2", "(\\d{4})(\\d{2})(\\d{2})", "$1-$2-$3"))
    )
    df2.show()
    
    /*    
    val dateFormat = "yyyy-dd-MM"
    val cleanDateDF = spark.range(1).select(
        to_date(lit("2017-12-11"), "yyyy-dd-MM").alias("date"),
        to_date(lit("2017-20-12"), dateFormat).alias("date2"))
        
    cleanDateDF.show() 
    */

}