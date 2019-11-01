package com.examples.sparkairports;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Map;

public class AirportFunctions {
    //public static Broadcast<Map<Integer, String>> getAirportBroadcasted(JavaSparkContext sc, JavaRDD<String> airport) {
        //JavaPairRDD<Integer, String> airportPair = airport.mapToPair(s-)

    //}

    public static String getAirportData(int pos, String s, boolean isAirport) {
        if (isAirport) {
            return getFromAirport(pos, s);
        } else {
            return FlightFunctions.getFromFlight(pos, s);
        }
    }


    private static String getFromAirport(int pos, String s) {
        String strsub = s.split(",", 2)[pos];
        return strsub.substring(1, strsub.length() - 1);
    }
}
