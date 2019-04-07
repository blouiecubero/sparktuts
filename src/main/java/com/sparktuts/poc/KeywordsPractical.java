package com.sparktuts.poc;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class KeywordsPractical {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> inputFile = sc.textFile("s3n://cubes-hadoop/input.txt");

        // check the contents of the file:

//        inputFile.take(10)
//                 .forEach(val -> System.out.println(val));

        // Get all the Subtitles
        JavaRDD<String> subtitlesOnly = inputFile
                .filter(line -> !line.matches("-?\\d+(\\.\\d+)?"))          // Check if Line is a number
                .filter(line -> !line.contains("-->"));                           // Check if Line is not a timestamp (which is denoted by -->)

        // Check if it contains only the subtitles:

//        subtitlesOnly
//                .foreach(val -> System.out.println(val));

        // Get all the words and Filter all the Boring Words

        JavaRDD<String> eachWords = subtitlesOnly
                .flatMap(eachSubtitle -> Arrays.asList(eachSubtitle.split(" ")).iterator())         // Get each word
                .map(line -> line.replaceAll("\\p{Punct}", "").toLowerCase())           // Filter out the punctuations (. , etc)
                .filter(line -> line.length() > 0)                                                        // Filter out the blank lines
                .filter(line -> Util.isNotBoring(line));

//        eachWords.foreach(val -> System.out.println(val));

        // Put the words to tuples and count the top 10 words in descending order

        eachWords
                .mapToPair(eachWord -> new Tuple2<>(eachWord, 1L))                                       // Enclose each word to a tuple of word and count of 1
                .reduceByKey((val1, val2) -> val1 + val2)                                                // Get the sum of all words
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))                                    // Swap the count as the key
                .sortByKey(false)                                                                        // so the value would be sorted by Key in desceding order
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))                                    // Swap back the word as the key again
                .take(10)                                                                          // Take the top 10 words
                .forEach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));    // Then print it

        sc.close();

    }
}
