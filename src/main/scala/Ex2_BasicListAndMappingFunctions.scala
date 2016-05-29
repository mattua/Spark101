/**
 * Created by mattua on 26/05/2016.
 */


import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}


object Ex2_BasicListAndMappingFunctions {


  /*
  BEST TO DEBUG THROUGH THIS STEP BY STEP


   */
   def main(args: Array[String]): Unit = {

     val context = new SparkContext(new SparkConf().setMaster("local").setAppName("ScalaExample"))

    // This creates two separate RDD's even though the source data is identical
     val nums = context.parallelize(List(1,2,3))
     val nums2 = context.parallelize(List(1,2,3))

    // tell spark to keep this RDD in memory
     nums.cache()

    //Print all the elements of the collection
     nums.foreach(println)


     /*

      map transforms an RDD of length N into another RDD of length N.

       context.textFile("sdfsdf".map(_.length)
         textFile creates an RDD of lines in the file
         _.length means for each line in input RDD count the length of that
         line and map it to the output RDD

        flatMap (loosely speaking) transforms an RDD of length N into a collection of N collections, then flattens these into a single RDD of results
      */

     // map function each element x in the input gets mapped to element value x*x in the output collection
     val squares = nums.map(x => x*x)
     squares.foreach(println)
     /*
     1
     4
     9
     Because squares was generated from nums RDD, squares contains a reference to
     nums RDD in its deps field
     */


     val flatSquares = nums.flatMap(x => List(x*x))
     flatSquares.foreach(println)
     /*
     Function passed to flatMap must return a descendant of type Seq
     Same result

     1
     4
     9
      */

    //each input element can map to a List of output elements and flatmap outputs them all in 1D list
    nums.flatMap(x => List(x*x,2*x)).foreach(println)


     squares.count() // number of entries in RDD - causes evaluation of the transformations

     // will aggregate all the items in the 1 dimensional RDD to one value
     System.out.println(squares.reduce(_+_)) // print the of items

     // print RDD by iteration over elements
     squares.foreach(println)

     //collect into local collection and print
     squares.collect().foreach(println)

     // for large rdd's better to just print a subset
     squares.take(3).foreach(println)
   }
 }
