/**
 * Created by mattua on 26/05/2016.
 */


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object PageRankScala {


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

    val lines = context.textFile("src/main/resources/sample_error_log.log")

    System.out.println("booms")





  }



}
