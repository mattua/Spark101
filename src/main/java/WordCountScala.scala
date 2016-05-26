/**
 * Created by mattua on 26/05/2016.
 */


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._


object WordCountScala {


  //val sc = new SparkContext("local","jobName","sparkHome",Seq("job.jar"))


  /*

    Rank pages according to how many links

    start each page on rank of 1

    each iteration -
      have page p contribute rankp/neighbours to its neighbours
      set page's rank (all) to 0.15 + 0.85 * contribs




   */



  def main(args: Array[String]): Unit ={

    val context= new SparkContext(new SparkConf().setMaster("local").setAppName("ScalaExample"))

    System.out.println("booms")

    System.out.println(context.textFile("src/main/resources/mary.txt").flatMap(theLine => theLine.split(" ")).map(theWord => (theWord,1)).collect())


    System.out.println(context.textFile("src/main/resources/mary.txt")
      .flatMap(theLine => theLine.split(" "))
      .map(theWord => (theWord,1)).collect().toString)





    val b = context.textFile("src/main/resources/mary.txt").flatMap(theLine => theLine.split(" ")).map(theWord => (theWord,1)).reduceByKey(_+_)

    // print RDD
    b.foreach(println)

    //collect into local collection and print
    b.collect().foreach(println)

    // for large rdd's better to just print a subset
    b.take(3).foreach(println)

    System.out.println("finished")

  }



}
