/**
 * Created by mattua on 26/05/2016.
 */


import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}


object StartSpark {


  //val sc = new SparkContext("local","jobName","sparkHome",Seq("job.jar"))


  /*

   map transforms an RDD of length N into another RDD of length N.

    context.textFile("sdfsdf".map(_.length)
      textFile creates an RDD of lines in the file
      _.length means for each line in input RDD count the length of that
      line and map it to the output RDD

flatMap (loosely speaking) transforms an RDD of length N into a collection of N collections, then flattens these into a single RDD of results



   */



  def main(args: Array[String]): Unit = {

    val context = new SparkContext(new SparkConf().setMaster("spark://MacBook-Pro.local:7077").setAppName("ScalaExample"))


     while (true){

       //See the GUI here
       // http://192.168.0.7:4041/jobs/
     }




  }
}
