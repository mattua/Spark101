/**
 * Created by mattua on 26/05/2016.
 */


import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}


object Ex4_WordCountMapReduce {



  def main(args: Array[String]): Unit ={

    val context= new SparkContext(new SparkConf().setMaster("local").setAppName("ScalaExample"))

    val a = context.textFile("src/main/resources/mary.txt")
    a.foreach(println)
    /*
      generates a MappedRDD and each element is a line of text
     */

    val a1 = a.map(line => line.split(" "))
    a1.foreach(s => {

      // each value in this RDD is a String array separated by space
      val h = s

    }) // creates an output RDD of string arrays, one for each line
    // So RDD of N for input of N lines



    /*

     map transforms an RDD of length N into another RDD of length N.

      context.textFile("sdfsdf".map(_.length)
        textFile creates an RDD of lines in the file
        _.length means for each line in input RDD count the length of that
        line and map it to the output RDD

    flatMap (loosely speaking) transforms an RDD of length N into a collection of N collections, then flattens these into a single RDD of results



     */
    val a2 = a.flatMap(line => line.split(" "))
    // creates collection of N String arrays (one for each line input)
    // then flattens them all out in a row into a 1D list
    a2.foreach(println)
    /*
    Flattens each line split by space into one long list

    Mary
    had
    a
    little
    lamb
    her
    fleece
    was
    white
    as
    snow
    and
    everywhere
    that
    Mary
    went
    etc
    etc
    */

    val a3 = a.flatMap(line => line.split(" ")).map(word => (word,1))
    a3.foreach(println)
    /*
    Takes each input item and puts it in a tuple with number 1

        (Mary,1)
        (had,1)
        (a,1)
        (little,1)
        (lamb,1)
        (her,1)
        (fleece,1)
        (was,1)
        (white,1)
        (as,1)
        (snow,1)
        (and,1)
        (everywhere,1)
        (that,1)
        (Mary,1)
        (went,1)
        (etc,1)
        (etc,1)


     */

    val a4 = a.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_)
    a4.foreach(println)
    /*

      groups the tuples by key and uses summing aggregation

      (went,1)
      (white,1)
      (etc,2)
      (was,1)
      (had,1)
      (fleece,1)
      (that,1)
      (a,1)
      (as,1)
      (Mary,2)
      (everywhere,1)
      (and,1)
      (snow,1)
      (lamb,1)
      (little,1)
      (her,1)



     */


  }



}
