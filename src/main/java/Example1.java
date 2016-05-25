package main.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;


/**
 * Created by matt.thomas on 23/05/2016.
 */
public class Example1 {



    private static final FlatMapFunction<String, String> WORDS_EXTRACTOR =
            new FlatMapFunction<String, String>() {

                public Iterable<String> call(String s) throws Exception {
                    return Arrays.asList(s.split(" "));
                }
            };

    private static final PairFunction<String, String, Integer> WORDS_MAPPER =
            new PairFunction<String, String, Integer>() {

                public Tuple2<String, Integer> call(String s) throws Exception {
                    return new Tuple2<String, Integer>(s, 1);
                }
            };

    private static final Function2<Integer, Integer, Integer> WORDS_REDUCER =
            new Function2<Integer, Integer, Integer>() {

                public Integer call(Integer a, Integer b) throws Exception {
                    return a + b;
                }
            };

    public static void main(String[] args) {

        System.out.println("Boom");

        SparkConf conf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> file = context.textFile("src/main/resources/alice-in-wonderland.txt");
        JavaRDD<String> words = file.flatMap(WORDS_EXTRACTOR);
        JavaPairRDD<String, Integer> pairs = words.mapToPair(WORDS_MAPPER);
        JavaPairRDD<String, Integer> counter = pairs.reduceByKey(WORDS_REDUCER);

        Map<String,Integer> values = counter.collectAsMap();

        int wordCount = 0;
        for (String s:values.keySet()){

            int count = values.get(s);
            System.out.println(s +" "+count);

            wordCount+=count;
        }
        System.out.println("total: "+ wordCount);



    }

}
