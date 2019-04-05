package com.sparktuts.poc;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Logs {
    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0405");
        inputData.add("FATAL: Wednesday 5 September 0405");
        inputData.add("ERROR: Friday 7 September 0405");
        inputData.add("WARN: Saturday 4 September 0405");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Convert data to RDD
        JavaRDD<String> originalLogMessages = sc.parallelize(inputData);

        // Calculate how many ERROR, WARN and FATAL there is
//        JavaPairRDD<String, Long> pairRDD = originalLogMessages.mapToPair(rawValue -> {
//            String[] columns = rawValue.split(":");
//            String level = columns[0];
//            String date = columns[1];
//            return new Tuple2<>(level, 1L);
//        });
//
//        JavaPairRDD<String, Long> sumsRdd = pairRDD.reduceByKey( (value1, value2) -> value1 + value2);
//
//        sumsRdd.foreach( tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

        // The shorter version of the calculation:

        sc.parallelize(inputData)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                .reduceByKey((value1, value2) -> value1 + value2)
                .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

        // group by key (WARNING: Could be problematic)
//        sc.parallelize(inputData)
//                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
//                .groupByKey()
//                .foreach(tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2)  + " instances"));

        // flatmaps and filters
        originalLogMessages
                .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .filter(word -> word.length() > 1)
                .foreach(word -> System.out.println(word));

        // Close the connection
        sc.close();

    }
}
