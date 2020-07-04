package tekcrux

//import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession


object DataFrameOps {
  def main(args: Array[String]) {
        
    // A directory in which you have winutils.exe file in a bin folder (bin\\winutils.exe)
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    val spark = SparkSession
                .builder
                .master("local[2]")
                .appName("DataFrameOps")
                .getOrCreate()
      
    //spark.conf.set("spark.sql.shuffle.partitions", 6)
    //spark.conf.set("spark.executor.memory", "2g")
             
    import spark.implicits._
            
    spark.sparkContext.setLogLevel("ERROR")
       
    //val df = spark.read.json("people.json")
    
    val df = spark.read.format("json").load("people.json")
        
    /*
    df.show()    
    df.printSchema() 
    println( df.columns.mkString(",") )
    
               
    val df2 = df.select("name", "age")
    df2.printSchema()
    df2.show()
    
    df.select($"name", $"age" + 1).show()    
    df.groupBy("age").count().show()
    
    /*val df_group = df.groupBy("age").count()
    df_group.printSchema()
    df_group.show(5)*/
    
    df.where("age > 21").show()      // where or filter
    df.orderBy("age").show()         // orderBy or sort
    */  
    
    // Local Temp View
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT name, age FROM people WHERE age IS NOT NULL ORDER BY age, name")
    sqlDF.show()   
    
    val newSession = spark.newSession()    
        
    // GlobalTempView
    df.createGlobalTempView("people2")
    spark.sql("SELECT * FROM global_temp.people2").show()
    
    // Global temporary view is cross-session
    newSession.sql("SELECT * FROM global_temp.people2 ORDER BY name").show()
    
    
    spark.stop()
    newSession.stop()  
    
  }
}