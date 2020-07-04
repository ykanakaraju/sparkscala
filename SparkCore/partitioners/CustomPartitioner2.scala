package partitioners

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.Partitioner

/*
 To implement a custom partitioner, we need to extend the 
 Partitioner class and implement the three methods which are:
 numPartitions : Int : returns the number of partitions 
 getPartitions(key : Any) : Int :  returns the partition ID (0 to numPartitions-1) for the given key
 equals() : This is the standard Java equality method. 
           Spark will need to test our partitioner object against other instances of itself 
           when it decides whether two of our RDDs are partitioned the same way!! 
 
 */
object CustomPartitioner2 extends App {
  
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("partitionBy").setMaster("local")    
    val sc = new SparkContext(conf)
    
    val rdd = sc.parallelize(Array(("USA", 81), ("USA", 82), ("India", 81),
                               ("UK", 81), ("India", 74), ("India", 79),
                               ("USA", 78), ("USA", 73), ("India", 74),
                               ("UK", 56), ("UK", 59), ("UK", 55),
                               ("Japan", 66), ("Japan", 69), ("Japan", 65),
                               ("France", 36), ("France", 39), ("France", 35),
                               ("Japan", 46), ("Japan", 49), ("Japan", 45),
                               ("France", 16), ("France", 19), ("France", 15),
                               ("Italy", 26), ("Italy", 29), ("Italy", 25)), 5)  
    sc.setLogLevel("ERROR")
    
    val customPartitioner = new MyCustomPartitioner2(4)
    
    val partitionedRdd = rdd.partitionBy(customPartitioner)
    
    partitionedRdd.foreachPartition( iter => 
          {
             print("Partition: ")
             iter.foreach(x => print(x.toString()))
             println()
          })
}

class MyCustomPartitioner2 (override val numPartitions : Int) extends Partitioner {
   
   override def getPartition(key: Any) : Int = {
      val k = key.asInstanceOf[String]
      //return k % numPartitions   
      k match {
        case "USA" => 0
        case "India" => 1
        case _ => 2 + key.hashCode() % (numPartitions - 2)
      }
   }
   
   override def equals(other: Any) : Boolean = {
     other match {
       case obj : MyCustomPartitioner => obj.numPartitions == numPartitions
       case _ => false
     }
   }
}