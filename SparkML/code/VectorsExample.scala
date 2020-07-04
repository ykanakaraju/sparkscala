package scala.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector, Vectors}

object VectorsExample extends App {
  
   val spark = SparkSession
      .builder
      .appName("EstimTransf")
      .master("local[2]")
      .getOrCreate()
     
   val denseHouse = Vectors.dense(4500d, 41000d, 0d, 0d, 4d)
   
   val sparseHouse = Vectors.sparse(5, Array(0,1,4), Array(4500d,41000d,4d))
   
   val zeroes = Vectors.zeros(3)    
   
   println("=== denseHouse ===")
   denseHouse.foreachActive((i, d) => println(s"$i, $d"))
   
   println("=== sparseHouse ===")
   sparseHouse.foreachActive((i, d) => println(s"$i, $d"))
   
   val nonZeros = sparseHouse.numNonzeros
   println("=== nonZeros ===")   
   println(s"nonZeros: $nonZeros")  
   
   val sparse = denseHouse.toSparse
   println("=== sparse ===")
   sparse.foreachActive((i, d) => println(s"$i, $d"))
   
}