package com.sparktuts.poc;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.HashMap;
import java.util.Map;

public class ViewingFiguresStructured {
    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("viewingFigures").setMaster("local[*]");

        SparkSession session = SparkSession.builder()
                                         .master("local[*]")
                                         .appName("structuredViewingReport")
                                         .getOrCreate();



        Dataset<Row> df = session.readStream().format("kafka")
                                              .option("kafka.bootstrap.servers", "localhost:9092")
                                              .option("subscribe", "viewrecords")

                                              .load();

        df.createOrReplaceTempView("viewing_figures");

        Dataset<Row> results = session.sql("select window, cast (value as string) as course_name, sum(5) as viewing_minutes from viewing_figures " +
                                            "group by window ( timestamp, '2 minutes' ), course_name order by viewing_minutes");

        StreamingQuery query = results.writeStream()
                                .format("console")
                                .outputMode(OutputMode.Complete())
                                .option("truncate", false)
                                .option("numRows", 20)
                                .start();

        query.awaitTermination();
    }

}
