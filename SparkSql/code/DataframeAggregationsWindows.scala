package tekcrux

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date, avg}

case class Salary(depName: String, empNo: Long, salary: Long)

object DataframeAggregationsWindows extends App {
  
    val spark = SparkSession
      .builder.master("local[2]")
      .appName("DataSourceBasic")
      .getOrCreate()
      
    spark.sparkContext.setLogLevel("ERROR")
    
    import spark.implicits._
    
    // Example 1
    val empsalary = Seq(
          Salary("sales", 1, 5000),
          Salary("personnel", 2, 3900),
          Salary("sales", 3, 4800),
          Salary("sales", 4, 4800),
          Salary("personnel", 5, 3500),
          Salary("develop", 7, 4200),
          Salary("develop", 8, 6000),
          Salary("develop", 9, 4500),
          Salary("develop", 10, 5200),
          Salary("develop", 11, 5200)).toDS

    import org.apache.spark.sql.expressions.Window

    val byDepName = Window.partitionBy("depName")
    println(byDepName)
    
    empsalary.show()
    
    empsalary.withColumn("avg", avg(col("salary")) over byDepName).show
    
    println("=============================================")
    


    // Example 2
    val df = spark.read.format("csv")
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .load("./data/retail-data/all/*.csv")
                  .coalesce(5)
                  
    df.cache()
    df.createOrReplaceTempView("dfTable")
    df.printSchema()
     
    val windowSpec = Window.partitionBy("CustomerID")
                           .orderBy(col("Quantity").desc)
                           .rowsBetween(Window.unboundedPreceding, Window.currentRow)
                              
    val dfWithCustAvgQty = df.withColumn("cutomerIdAvgQty", avg(col("Quantity")) over windowSpec)
    dfWithCustAvgQty.select("CustomerID", "Quantity", "cutomerIdAvgQty").show(100)
    //dfWithCustAvgQty.createOrReplaceTempView("dfWithCustAvgQty")
        
    import org.apache.spark.sql.functions.max
    val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

    import org.apache.spark.sql.functions.{dense_rank, rank}
    val purchaseDenseRank = dense_rank().over(windowSpec)
    val purchaseRank = rank().over(windowSpec)
    
    dfWithCustAvgQty.where("CustomerId IS NOT NULL")
                    .orderBy("Quantity")
                    .select(
                      col("CustomerId"),
                      col("InvoiceDate"),
                      col("Quantity"),
                      col("cutomerIdAvgQty"),
                      purchaseRank.alias("quantityRank"),
                      purchaseDenseRank.alias("quantityDenseRank"),
                      maxPurchaseQuantity.alias("maxPurchaseQuantity")).show(10)


}