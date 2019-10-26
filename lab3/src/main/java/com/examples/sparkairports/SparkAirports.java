package com.examples.sparkairports;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Map;

public class SparkAirports {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("lab3doubl");
        JavaSparkContext sc = new JavaSparkContext(conf);


        //JavaSparkContext.textFile


        final Broadcast<Map<String, AirportData>> airportsBroadcasted =
                sc.broadcast(stringAirportDataMap);
    }
}
