import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple1;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by mattua on 26/05/2016.
 */
public class ExampleYouTube {


    public static void main(String[] args) throws Exception {


        System.out.println("booms");


        SparkConf conf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local");



        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> lines = context.textFile("src/main/resources/sample_error_log.log");

        long totalErrors = lines.filter(
                new Function<String, Boolean>() {
                    public Boolean call(String s) throws Exception {
                        return s.contains("error");
                    }
                }


        ).count();




        System.out.println("Number of error log messages " + totalErrors);


        // create an RDD from a List
        JavaRDD<Integer> nums = context.parallelize(Arrays.asList(1, 2, 3));

        System.out.println("retrived as local collection "+ nums.collect());

        // first k elements
        System.out.println("retrived first 2 elements "+ nums.take(2));

        //count elements
        System.out.println("retrived number of elements "+ nums.count());

        //merge elements with associattive function
        System.out.println("sum of elements "+ nums.reduce(new
           Function2<Integer, Integer, Integer>() {
               public Integer call(Integer integer, Integer integer2) throws Exception {
                   return integer+integer2;
               }
           }));


        // save RDD to text file
        //nums.saveAsTextFile("nums.txt");   // saves as folder, skip for now

        JavaRDD<Tuple2> pets = context.parallelize(Arrays.asList(
                new Tuple2("cat", 1),
                new Tuple2("dog", 1),
                new Tuple2("cat", 2)));

        //System.out.println(pets.groupBy());
        Thread.sleep(300000);

        /*
            during this time observe
                http://localhost:4040/jobs/ - see the jon named "count" there
                this is the first time we actually ran a jon - everything
                up to this point was defining transformations.

                Each time we perform an operation on an RDD it is submitted
                as a separate job
         */

    }


}
