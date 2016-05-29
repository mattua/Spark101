/**
 * Created by mattua on 26/05/2016.
 */


import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}


object Ex1_StartSparkAndViewJobConsole {

  def main(args: Array[String]): Unit = {

    val context = new SparkContext(new SparkConf().setMaster("local").setAppName("ScalaExample"))


    val sampleRDD1 = context.textFile("src/main/resources/mary.txt").flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_)

    Thread.sleep(30000);
    /*

    See the GUI here
    http://localhost:4040/jobs/

    Notice there are no jobs - we have not done any processing, just defined the transformations

    */



    val sampleRDD2 = context.textFile("src/main/resources/mary.txt")
      .flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_).foreach(println)

    Thread.sleep(30000);
    /*

    See the GUI here
    http://localhost:4040/jobs/

    Now we can see that a job has been executed  - the file has been loaded and the tranformations
    have been applied to the RDD, creating a new RDD from each method call - each chained operation
    transforms the data further.

    */

  }
}
