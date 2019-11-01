package com.examples.sparkairports;

import javafx.util.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

public class FlightFunctions {
    private static final int MAX_DELAY_POS = 0;
    private static final int AIRPORT_ID_TO_POS = 14;
    private static final int AIRPORT_ID_FROM_POS = 11;
    private static final int DELAY_FLIGHT_POS = 17;
S    private static final int IS_CANCELLED_POS = 1;
    private static final int NUMBER_OF_DELAY_POS = 2;
    private static final int NUMBER_OF_CANCELLED_POS = 3;
    private static final int NUMBER_OF_FLIGHTS_POS = 4;

    public static String getFromFlight(int pos, String s) {
        return s.split(",")[pos];
    }

    public static JavaPairRDD<Pair<Integer, Integer>, String> handleFlight(JavaRDD<String> flights) {
        JavaPairRDD<Pair<Integer, Integer>, float[]> flightPair = ;
        JavaPairRDD<Pair<Integer, Integer>, float[]> flightFiltered = filterFlights(flightPair);
        JavaPairRDD<Pair<Integer, Integer>, float[]> flightPair = ;


    }

    private static JavaPairRDD<Pair<Integer, Integer>, float[]> filterFlights(JavaPairRDD<Pair<Integer, Integer>, float[]> flights) {
        return flights.filter(pair -> pair._2[MAX_DELAY_POS] >=0);
    }
}

