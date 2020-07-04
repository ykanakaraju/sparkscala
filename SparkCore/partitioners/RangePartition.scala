package partitioners

import org.apache.spark.RangePartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object RangePartition extends App {
  
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("partitionBy").setMaster("local")    
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR") 
    
    //val rdd = sc.parallelize(Seq((1, 11), (4,44), (5,55), (7,77), (8,88), (6,66), (2,22), (3,33),  (9,99), (10,1010)), 3)
    
    val rdd = sc.parallelize(Array(('A', 14), ('W',44), ('D',55), ('B',77), ('E',88), ('X',66), 
                                   ('Y',22), ('Z',33),  ('M',99), ('L',1010)), 3)
    rdd.mapPartitionsWithIndex( (i, iter) => Iterator((i, iter.toMap))).collect.foreach(println)
        
    println( "Number of Partitions: " + rdd.getNumPartitions )
    println( "Partitioner: " + rdd.partitioner.toString() )      
     
    
    val rangePartitioner = new RangePartitioner(3, rdd)
    
    val partitionedRdd = rdd.partitionBy(rangePartitioner).persist()
    
    partitionedRdd.mapPartitionsWithIndex( (i, iter) => Iterator((i, iter.toMap))).collect.foreach(println)
 
    println( "Number of Partitions: " + partitionedRdd.getNumPartitions )
    println( "Partitioner: " + partitionedRdd.partitioner.toString() ) 
    
    
}