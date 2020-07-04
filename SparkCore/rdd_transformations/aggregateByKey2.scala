package rdd_transformations

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.collection.mutable

object aggregateByKey2 extends App {
  
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    val conf = new SparkConf().setAppName("aggregate by key").setMaster("local")
    val sc = new SparkContext(conf)   
          
    sc.setLogLevel("ERROR")
    
    val students_list = List(
  ("Aditya", "Maths", 83), ("Aditya", "Physics", 74), ("Aditya", "Chemistry", 91), ("Aditya", "English", 82), 
  ("Amrita", "Maths", 69), ("Amrita", "Physics", 62), ("Amrita", "Chemistry", 97), ("Amrita", "English", 80), 
  ("Pranav", "Maths", 78), ("Pranav", "Physics", 73), ("Pranav", "Chemistry", 68), ("Pranav", "English", 87), 
  ("Keerti", "Maths", 87), ("Keerti", "Physics", 93), ("Keerti", "Chemistry", 91), ("Keerti", "English", 74), 
  ("Harsha", "Maths", 56), ("Harsha", "Physics", 65), ("Harsha", "Chemistry", 71), ("Harsha", "English", 68), 
  ("Anitha", "Maths", 86), ("Anitha", "Physics", 62), ("Anitha", "Chemistry", 75), ("Anitha", "English", 83), 
  ("Komala", "Maths", 63), ("Komala", "Physics", 69), ("Komala", "Chemistry", 64), ("Komala", "English", 60))
  
    val student_rdd = sc.parallelize(students_list , 3)
    val aggr_rdd = student_rdd.map( t => (t._1, (t._2, t._3)))
    
    /*
    // Ex 1: Print Max Marks per Student                 
    // accumulator: 0, element: (subject, marks)         
    */
    
    val zero_val = 0
    
    def seq_op(accumulator: Int, element: (String, Int) ) = {      
      if(accumulator > element._2) accumulator 
      else element._2
    }
    
    def comb_op(accumulator1: Int, accumulator2: Int) = {
      if(accumulator1 > accumulator2) accumulator1 
      else accumulator2
    }    
    
    val max_marks_rdd = aggr_rdd.aggregateByKey(zero_val)(seq_op, comb_op) 
    max_marks_rdd.foreach( println )

    
    println("\n=================\n")

    /*
    // Ex 2: Print Subject name along with Max Marks                
    // accumulator: ('', 0), element: (subject, marks)     
    */
    
    val zero_val_2 = ("", 0)
    
    def seq_op_2(accumulator: (String, Int), element: (String, Int) ) = {      
      if(accumulator._2 > element._2) accumulator 
      else element
    }
    
    def comb_op_2(accumulator1: (String, Int), accumulator2: (String, Int)) = {
      if(accumulator1._2 > accumulator2._2) accumulator1 
      else accumulator2
    }    
    
    val max_marks_subj_rdd = aggr_rdd.aggregateByKey(zero_val_2)(seq_op_2, comb_op_2)
    max_marks_subj_rdd.foreach( println )        
    
}