package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object sortbyKey {
  def main(args: Array[String]) {
    
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
     
    val inputfile = "data/Baby_Names.csv";  //args(0)
    val conf = new SparkConf().setAppName("sort").setMaster("local")
    
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val babyNames = sc.textFile(inputfile)

    val filteredRows = babyNames.filter(line => !line.contains("Count"))
                                .map(line => line.split(","))
    
        
    //val sort_output = filteredRows.map(n => (n(1), n(4))).sortByKey().foreach(println _)
    
    val sort_Desc = filteredRows.map(n => (n(1), n(4)))
                                .sortByKey( false )
                                .take(5)
                                .foreach(println _) // opposite order
    
    //val savedata = filteredRows.map(n => (n(1), n(4))).sortByKey(false)
    //savedata.saveAsTextFile(outputfile)
    //println("Sorted in Ascending" + sort_output)
    
    
    
  }
}