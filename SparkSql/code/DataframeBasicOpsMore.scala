package basics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, Encoder}
import org.apache.commons.io.FileUtils;
import java.io.File;

object DataframeBasicOpsMore extends App {
  
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("DataSourceBasic")
      .getOrCreate()
   
   spark.sparkContext.setLogLevel("ERROR")
   
   import spark.implicits._

   val listUsers = Seq((1, "Raju", 5),
                       (2, "Ramesh", 15),
                       (3, "Rajesh", 18),
                       (4, "Raghu", 35),
                       (5, "Ramya", 25),
                       (6, "Radhika", 35),
                       (7, "Ravi", 70))

  //val usersDf = spark.createDataFrame(listUsers).toDF("id", "name", "age")
  val usersDf = listUsers.toDF("id", "name", "age")
  usersDf.show()
  usersDf.printSchema()
  
  //----------------------------------------
  //   1. Creating DataFrame from RDDs
  //----------------------------------------
  println("---- Creating DataFrame from RDD ----")
  val rdd1 = spark.sparkContext.parallelize(listUsers, 1)
  
  val df1 = rdd1.toDF("id", "name", "age")
  
  df1.show()
  df1.printSchema()
  
  //---------------------------------------- 
  //  2. 'when .. otherwise' 
  //---------------------------------------- 
  println("---- when .. otherwise ----")    
  val df2 = df1.withColumn("ageGroup", when(df1.col("age") <= 12, "child")
                           .when(df1.col("age") <= 19, "teenager")
                           .when(df1.col("age") <= 60, "adult")
                           .otherwise("senior"))                           
   df2.show()                           
  
   println("---- case .. when ----")   
   val case_when = """case when age <= 12 then 'child'
	                         when age <= 19 then 'teenager' 
	                         when age <= 60 then 'adult'
	                         else 'senior' 
	                     end"""
   
   val df3 = df1.withColumn("ageGroup", expr(case_when))
   df3.show()
   
    //---------------------------------------- 
    //  3. 'dropDuplicates' 
    //---------------------------------------- 
    println("---- dropDuplicates ----")   
    val users3 = Seq((1, "Raju", 5),
                     (1, "Raju", 5),
                     (3, "Raju", 5),
                     (4, "Raghu", 35),
                     (4, "Raghu", 35),
                     (6, "Raghu", 35),
                     (7, "Ravi", 70))

    val users3Df = spark.createDataFrame(users3).toDF("id", "name", "age")
   
    val users3Df2 = users3Df.dropDuplicates()
    users3Df2.show()
   
    val users3Df3 = users3Df.dropDuplicates("name", "age")
    users3Df3.show()
    
    //---------------------------------------- 
    //  4. 'na functions' 
    //---------------------------------------- 
     println("---- na functions ----")   
     val users4Df = spark.read.json("E:\\PySpark\\data\\users.json")
     users4Df.show()

     users4Df.na.drop().show()
     //users4Df.na.drop( Seq("phone") ).show()
     
     // fills String columns with "NA" and int columns with 0
     users4Df.na.fill("NA").na.fill(0).show()  

     //---------------------------------------- 
     //  5. 'udf' 
     //---------------------------------------- 
     println("---- udf ----")   
          
     val users5 = Seq((1, "Raju", 5),
                       (2, "Ramesh", 15),
                       (3, "Rajesh", 18),
                       (4, "Raghu", 35),
                       (5, "Ramya", 25),
                       (6, "Radhika", 35),
                       (7, "Ravi", 70))

     val users5Df = spark.createDataFrame(users5)
                         .toDF("id", "name", "age") 
                         
     users5Df.show()   
     
     val get_age_group = (age: Int) => {
        if (age <= 12) "child"
        else if (age <= 18) "teenager"
        else if (age <= 60) "adult"
        else "senior"            
     }
    
     val get_age_group_udf = udf(get_age_group)
     
     val users5Df2 = users5Df.withColumn("ageGroup", get_age_group_udf(col("age")))
     
     users5Df2.show()
     
     spark.udf.register("get_age_group_udf", get_age_group)
     
     spark.catalog
          .listFunctions()
          .select("name")
          .where("name like '%_udf'")
          .show(false)
          
     //spark.catalog.dropTempView("users")
     users5Df2.createOrReplaceTempView("users")
     
     spark.sql("""select id, name, age, get_age_group_udf(age) as ageGroup 
                  from users""")
          .show()          
}

