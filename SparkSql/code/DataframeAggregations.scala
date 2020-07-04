package tekcrux

import org.apache.spark.sql.SparkSession

object DataframeAggregations extends App {
  
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    val spark = SparkSession
      .builder.master("local[2]")
      .appName("DataSourceBasic")
      .getOrCreate()
      
    spark.sparkContext.setLogLevel("ERROR")
    
    import spark.implicits._
       
    val df = spark.read
                  .format("csv")            //.option("sep", "\t")  
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .load("./data/retail-data/all/*.csv")
                  .coalesce(5)
                  
    df.cache()
    df.createOrReplaceTempView("dfTable")    
    df.printSchema()
        
    val totalCount = df.count()
    println(s"totalCount: $totalCount")
       
    // ----------------------------------------------------------
    // Example 1 - count (transformation)
    // ----------------------------------------------------------
    printSeparator("Example 1 - count")
    import org.apache.spark.sql.functions.count
    df.select(count("StockCode")).show() 
    
    //df.selectExpr("count(\"StockCode\") as count").show() 
    //spark.sql("SELECT COUNT(*) as count FROM dfTable").show()

    // ----------------------------------------------------------
    // Example 2 - countDistinct
    // ----------------------------------------------------------
    printSeparator("Example 2 - countDistinct")
    import org.apache.spark.sql.functions.countDistinct
    df.select(countDistinct("StockCode")).show() 
    //spark.sql("SELECT COUNT(DISTINCT StockCode) as count FROM dfTable").show()

    // ----------------------------------------------------------
    // Example 3 - approx_count_distinct
    // ----------------------------------------------------------
    printSeparator("Example 3 - approx_count_distinct")
    import org.apache.spark.sql.functions.approx_count_distinct
    df.select(approx_count_distinct("StockCode", 0.1)).show()
    //spark.sql("SELECT approx_count_distinct(StockCode, 0.1) as count FROM dfTable").show()
    
    // ----------------------------------------------------------
    // Example 4 - first, last, min & max
    // ----------------------------------------------------------
    printSeparator("Example 4 - first, last, min & max")
    import org.apache.spark.sql.functions.{first, last, min, max}
    df.select(first("StockCode"), last("StockCode")).show()
    df.select(min("Quantity"), max("Quantity")).show()
    
    /*spark.sql("""SELECT first(StockCode) as first, 
                        last(StockCode) as last,
                        min(Quantity)  as minQty,
                        max(Quantity) as maxQty
                 FROM dfTable""").show() */
                 
    // ----------------------------------------------------------
    // Example 5 - sum, sumDistinct & avg/mean
    // ----------------------------------------------------------
    printSeparator("Example 5 - sum, sumDistinct & avg/mean")
    import org.apache.spark.sql.functions.{sum, sumDistinct, avg, mean, expr}
    df.select(sum("Quantity")).show() 
    df.select(sumDistinct("Quantity")).show()
    df.select(avg("Quantity")).show()
    
    /*spark.sql("""SELECT sum(Quantity)  as sumQty, 
                        mean(Quantity) as mean
                 FROM dfTable""").show()*/ 
                 
    df.select(
      count("Quantity").alias("total_transactions"),
      sum("Quantity").alias("total_purchases"),
      avg("Quantity").alias("avg_purchases"),
      expr("mean(Quantity)").alias("mean_purchases"))
    .selectExpr(
      "total_purchases/total_transactions",
      "avg_purchases",
      "mean_purchases").show()             
    
    // ----------------------------------------------------------
    // Example 6 - variance and standard deviation
    // ----------------------------------------------------------
    printSeparator("Example 6 - varience and standard deviation")             
    import org.apache.spark.sql.functions.{var_pop, stddev_pop, variance, stddev}
    import org.apache.spark.sql.functions.{var_samp, stddev_samp}
    
    df.select(variance("Quantity"), stddev("Quantity"),
              var_pop("Quantity"), stddev_pop("Quantity"),
              var_samp("Quantity"), stddev_samp("Quantity"))
       .show()
    
    /*spark.sql("""SELECT var_pop(Quantity), 
                        var_samp(Quantity),
                        stddev_pop(Quantity), 
                        stddev_samp(Quantity)
                 FROM dfTable""").show()*/
              
                
    // ----------------------------------------------------------
    // Example 7 - skewness & kurtosis
    // ----------------------------------------------------------
    printSeparator("Example 7 - skewness & kurtosis")  
    import org.apache.spark.sql.functions.{skewness, kurtosis}
    df.select(skewness("Quantity"), kurtosis("Quantity")).show()             
    spark.sql("SELECT skewness(Quantity), kurtosis(Quantity) FROM dfTable").show()             
    
    // ----------------------------------------------------------
    // Example 8 - Covariance and Correlation
    // ----------------------------------------------------------
    printSeparator("Example 8 - Covariance and Correlation") 
    
    import org.apache.spark.sql.functions.{corr, covar_pop, covar_samp}

    df.select(corr("InvoiceNo", "Quantity"), 
              covar_samp("InvoiceNo", "Quantity"),
              covar_pop("InvoiceNo", "Quantity")).show()
    
    spark.sql("""SELECT corr(InvoiceNo, Quantity), 
                        covar_samp(InvoiceNo, Quantity),
                        covar_pop(InvoiceNo, Quantity)
                  FROM dfTable""").show()
                 
    // ----------------------------------------------------------
    // Example 9 - Aggregation on Collections
    // ----------------------------------------------------------
    printSeparator("Example 9 - Aggregation on Collections")
    
    import org.apache.spark.sql.functions.{collect_set, collect_list}
    df.agg(collect_set("Country"), collect_list("Country")).show()
    
    spark.sql("SELECT collect_set(Country), collect_list(Country) FROM dfTable")
         .show()
    
    // ----------------------------------------------------------
    // Example 10 - Grouping
    // ----------------------------------------------------------
    printSeparator("Example 10 - Grouping")
    
    import org.apache.spark.sql.functions.count
    
    df.groupBy("InvoiceNo", "CustomerId").count().show()
    
    df.groupBy("InvoiceNo")
      .agg(min("Quantity").alias("min_quan"), expr("count(Quantity)"))
      .show(5)
      
    df.groupBy("InvoiceNo").agg("Quantity"->"avg", "Quantity"->"stddev_pop")
      .show(5) 
    

    // =================================================
    def printSeparator(str: String) {
      println()
      println( "-" * str.length())
      println( str )
      println( "-" * str.length())
    }
    
}