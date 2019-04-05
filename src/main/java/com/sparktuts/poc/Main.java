package com.sparktuts.poc;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
import scala.Tuple2;
import sun.rmi.server.InactiveGroupException;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Convert data to RDD
        JavaRDD<Integer> myRdd = sc.parallelize(inputData);

        Integer result = myRdd.reduce((value1, value2) -> value1 + value2);

        // Output sum:
        System.out.println(result);

        // sqrt all the elements of the array using map
        JavaRDD<Double> sqrtRdd = myRdd.map(value -> Math.sqrt(value));

        // output sqrt'd values:
        sqrtRdd.foreach(value -> System.out.println(value));

        // doing count via mapreduce:
        JavaRDD<Integer> singleIntegerRdd = sqrtRdd.map(value -> 1);
        Integer count = singleIntegerRdd.reduce((value1, value2) -> value1 + value2);
        System.out.println(count);

        // storing the integer and its sqrt in a single row
        // using tuples:

        JavaRDD<Tuple2<Integer, Double>> iws = myRdd.map(value -> new Tuple2<Integer, Double>(value, Math.sqrt(value)));

        // Close the connection
        sc.close();




    }
}
