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
object CustomPartitioner extends App {
  
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("partitionBy").setMaster("local")    
    val sc = new SparkContext(conf)
    
    val rdd = sc.parallelize(Seq((1, 1), (2,4), (3,9), (4,16), (1,2), (2,4), (3,6), (4,8), (5,10), (5,25), (6,12), (6,36), (2,4), (3,6), (4,8), (5,10), (5,25), (6,12), (6,36)))
    sc.setLogLevel("ERROR")
    
    val customPartitioner = new MyCustomPartitioner(7)
    
    val partitionedRdd = rdd.partitionBy(customPartitioner)
    
    partitionedRdd.foreachPartition( iter => 
          {
             print("Partition: ")
             iter.foreach(x => print(x.toString()))
             println()
          })
}

class MyCustomPartitioner(override val numPartitions : Int) extends Partitioner {
   
   override def getPartition(key: Any) : Int = {
      val k = key.asInstanceOf[Int]
      //return k % numPartitions   
      k match {
        case 1 => 0
        case 2 => 1
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