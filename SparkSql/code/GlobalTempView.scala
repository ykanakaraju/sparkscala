package tekcrux

import org.apache.spark.sql.SparkSession

object GlobalTempView extends App {
  
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    val spark = SparkSession
      .builder.master("local[2]")
      .appName("DataSourceBasic")
      .getOrCreate()
      
    spark.sparkContext.setLogLevel("ERROR")
    
    import spark.implicits._
    
    val df = spark.range(100).toDF()
    println(df.schema)
    df.printSchema()
    df.show(10)
    
    df.createOrReplaceTempView("MyTable")
    spark.sql("show tables").show()
    spark.sql("select * from MyTable").show(5)
    
    //spark.catalog.listTables().show()
    
    // you can not see the above table using this session
    val newSession = spark.newSession()
    newSession.sql("show tables").show()
    //newSession.sql("select * from MyTable").show()
    
    df.createGlobalTempView("MyGlobalView")
    spark.sql("select * from global_temp.MyGlobalView").show(2)
    newSession.sql("select * from global_temp.MyGlobalView").show(2) 
    
    
    
    // Catelog
    
}