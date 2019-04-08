package com.sparktuts.poc;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SparkSQL {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                                                   .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                                                   .getOrCreate();

//        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

//        long numberOfRows = dataset.count();
//
//
//        dataset.show();
//        System.out.println("There are " + numberOfRows + " records");
//
//        Row firstRow = dataset.first();
//
//        String subject = firstRow.getAs("grade").toString();
//        System.out.println(subject);

        // Expressions Kind of Query
//        Dataset<Row> germanResults = dataset.filter(" subject = 'German' AND year >= 2007 ");
//        germanResults.show();


        // DSL (columnar) way of Querying
//        Dataset<Row> modernArtResults = dataset.filter( col("subject").equalTo("Modern Art")
//                                                        .and(col("year").geq(2007)));
//        modernArtResults.show();

        // SQL Style of Querying
//        dataset.createOrReplaceTempView("my_students_table");
//
//        Dataset<Row> results = spark.sql("select * from my_students_table where subject='French'");
//        results.show();


        // In memory data loading

        List<Row> inMemory = new ArrayList<Row>();
        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

        StructField[] fields = new StructField[] {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };

        StructType schema = new StructType(fields);
        Dataset<Row> logDataset = spark.createDataFrame(inMemory, schema);
        logDataset.show();


        spark.close();
    }
}
