/**
 * Created by mattua on 26/05/2016.
 */

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}


object Ex3_MapAndGroupByKey {


   def main(args: Array[String]): Unit = {

     val context = new SparkContext(new SparkConf().setMaster("local").setAppName("ScalaExample"))

     val pets = context.parallelize(List(("cat",1),("dog",1),("cat",2)))

     // does exactly the same as above, but groups by key before doing the sum
     pets.reduceByKey(_+_).foreach(println)


     pets.groupByKey().foreach(println)
     /*
     (dog,CompactBuffer(1))
     (cat,CompactBuffer(1, 2))

     groups the tuples by key
      */

     val c = pets.groupByKey()
     /*
       generates a ShuffledRDD from the ParallelRDD

      */
   }
 }
