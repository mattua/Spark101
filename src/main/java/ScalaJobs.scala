/**
 * Created by mattua on 26/05/2016.
 */


import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}


object ScalaJobs {





  def main(args: Array[String]): Unit ={

    val context= new SparkContext(new SparkConf().setMaster("local").setAppName("ScalaExample"))


    val a = context.textFile("src/main/resources/mary.txt")



    val a4 = a.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_)
    a4.foreach(println)
    // Only one job is submitted per chain!!!! the job is named by the last method in the chain




    Thread.sleep(300000);
    System.out.println("finished")

  }



}
