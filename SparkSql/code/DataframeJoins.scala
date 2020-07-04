package tekcrux

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr}
object DataframeJoins extends App {
  
    val spark = SparkSession
        .builder.master("local[2]")
        .appName("DataSourceBasic")
        .getOrCreate()
      
    spark.sparkContext.setLogLevel("ERROR")
    
    import spark.implicits._
    
    val employee = Seq(
        (1, "Raju", 25, 101),
        (2, "Ramesh", 26, 101),
        (3, "Amrita", 30, 102),
        (4, "Madhu", 32, 102),
        (5, "Aditya", 28, 102),
        (6, "Aditya", 28, 10000)
       )
      .toDF("id", "name", "age", "deptid")
    
    val department = Seq(
        (101, "IT", 1),
        (102, "Opearation", 1),
        (103, "HRD", 2))
      .toDF("id", "deptname", "locationid")
    
    val locations = Seq(
        (1, "Bangalore", Seq("Bellandur", "KR Puram")),
        (2, "Hyderabad", Seq("Gachibouli", "Madhapur")),
        (4, "Pune", Seq("Hinjawadi")),
        (5, "Chennai", Seq("Tambaram", "Alwarpet")))
      .toDF("id", "city", "address")
      
    employee.show()
    department.show()
    //locations.show()
    //locations.printSchema()
    
    // temp views
    employee.createOrReplaceTempView("employee")
    department.createOrReplaceTempView("department")
    locations.createOrReplaceTempView("locations")
    
    // Example 1 - Inner Join
    val joinEmpDept = employee.col("deptid") === department.col("id")
    
    println("=== inner join ===")
    employee.join(department, joinEmpDept, "inner").show() 
    
    //employee.join(department, joinEmpDept).drop(department.col("id")).show() 
    //employee.join(department, joinEmpDept).show() 
    //
    
    /*
    val sql_ij = """SELECT * FROM employee JOIN department
                   ON employee.deptid = department.id"""
    
    spark.sql(sql_ij).show()
    */
    
    
    println("=== outer join ===")
    employee.join(department, joinEmpDept, "outer").show()
    
    /*
    val sql_foj = """SELECT * FROM employee FULL OUTER JOIN department
                   ON employee.deptid = department.id"""
    spark.sql(sql_foj).show()
    */    
        
    println("=== left outer join ===")
    employee.join(department, joinEmpDept, "left_outer").show() 
    
    println("=== right outer join ===")
    employee.join(department, joinEmpDept, "right_outer").show()
    
    println("=== left semi join ===")
    employee.join(department, joinEmpDept, "left_semi").show()
    
    println("=== left anti join ===")
    employee.join(department, joinEmpDept, "left_anti").show()
    
    //println("=== cross join ===")
    //department.join(employee, joinEmpDept, "cross").show()
        
    
    //broadcast join
    println("=== broadcast join ===")
    import org.apache.spark.sql.functions.broadcast
    val joinExpr = employee.col("deptid") === department.col("id")
    val bcDf = employee.join(broadcast(department), joinExpr)
    
    bcDf.show()
    bcDf.explain()
    
    
}