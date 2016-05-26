/**
 * Created by mattua on 26/05/2016.
 */


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._


object YouTubeExampleScala {


  //val sc = new SparkContext("local","jobName","sparkHome",Seq("job.jar"))


  /*

   map transforms an RDD of length N into another RDD of length N.

    context.textFile("sdfsdf".map(_.length)
      textFile creates an RDD of lines in the file
      _.length means for each line in input RDD count the length of that
      line and map it to the output RDD

flatMap (loosely speaking) transforms an RDD of length N into a collection of N collections, then flattens these into a single RDD of results



   */



  def main(args: Array[String]): Unit ={

    val context= new SparkContext(new SparkConf().setMaster("local").setAppName("ScalaExample"))


    //context.rd

    System.out.println("booms")


    val nums = context.parallelize(List(1,2,3))
    val nums2 = context.parallelize(List(1,2,3))

    nums.cache()
    val nums3 = context.parallelize(List(1,2,3))





    // note three completely separate ParallelCollectionRDD objects are created from above
    //

    nums.foreach(println)


    /*
    map generates N elements from N
     */
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


    squares.count() // number of entries in RDD - causes evaluation of the transformations

    // will aggregate all the items in the 1 dimensional RDD to one value
    squares.reduce(_+_)



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

    System.out.println(context.textFile("src/main/resources/mary.txt").flatMap(theLine => theLine.split(" ")).map(theWord => (theWord,1)).collect())


    System.out.println(context.textFile("src/main/resources/mary.txt")
      .flatMap(theLine => theLine.split(" "))
      .map(theWord => (theWord,1)).collect().toString)


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


    val b = context.textFile("src/main/resources/mary.txt").flatMap(theLine => theLine.split(" ")).map(theWord => (theWord,1)).reduceByKey(_+_)

    // print RDD
    b.foreach(println)

    //collect into local collection and print
    b.collect().foreach(println)

    // for large rdd's better to just print a subset
    b.take(3).foreach(println)

    Thread.sleep(300000);
    System.out.println("finished")

  }



}
