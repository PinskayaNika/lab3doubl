package com.examples.sparkairports;

import javafx.util.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

class FlightFunctions {
    private static final int MAX_DELAY_POS = 0;
    private static final int AIRPORT_ID_TO_POS = 14;
    private static final int AIRPORT_ID_FROM_POS = 11;
    private static final int DELAY_FLIGHT_POS = 17;
    private static final int IS_CANCELLED_POS = 1;
    private static final int NUMBER_OF_DELAYED_POS = 2;
    private static final int NUMBER_OF_CANCELLED_POS = 3;
    private static final int NUMBER_OF_FLIGHTS_POS = 4;

    static String getFromFlight(int pos, String s) {
        return s.split(",")[pos];
    }

    static JavaPairRDD<Pair<Integer, Integer>, String> handleFlight(JavaRDD<String> flights) {
        JavaPairRDD<Pair<Integer, Integer>, float[]> flightPair = createFlightsPair(flights);
        JavaPairRDD<Pair<Integer, Integer>, float[]> flightFiltered = filterFlights(flightPair);
        JavaPairRDD<Pair<Integer, Integer>, float[]> flightReduced = reduceFlights(flightFiltered);
        return dataToString(flightReduced);
    }

    private static JavaPairRDD<Pair<Integer, Integer>, float[]> createFlightsPair (JavaRDD<String> flights) {
        return flights.mapToPair(s -> {
            Pair<Integer, Integer> airportsID = new Pair<>(
                    Integer.parseInt(AirportFunctions.getAirportData(AIRPORT_ID_FROM_POS, s, false)),
                    Integer.parseInt((AirportFunctions.getAirportData(AIRPORT_ID_TO_POS, s, false)))
            );
            float[] delayData = new float[] {0, 0, 0, 0, 1};
            if(AirportFunctions.getAirportData(DELAY_FLIGHT_POS, s, false).length() > 0) {
                delayData[MAX_DELAY_POS] = Float.parseFloat(AirportFunctions.getAirportData(DELAY_FLIGHT_POS, s, false));
                delayData[NUMBER_OF_DELAYED_POS] = 1;
            } else {
                delayData[IS_CANCELLED_POS] = 1;
            }
            return new Tuple2<>(airportsID, delayData);
        });
    }

    private static JavaPairRDD<Pair<Integer, Integer>, float[]> filterFlights(JavaPairRDD<Pair<Integer, Integer>, float[]> flights) {
        return flights.filter(pair -> pair._2[MAX_DELAY_POS] >=0);
    }

    //С помощью функции reduce или аналогичных расчитываем максимальное
    //время опоздания, процент опоздавших+отмененных рейсов
    private static JavaPairRDD<Pair<Integer, Integer>, float[]> reduceFlights(JavaPairRDD<Pair<Integer, Integer>, float[]> flights) {
        return flights.reduceByKey((flightsInfo1, flightsInfo2) -> {
            flightsInfo1[NUMBER_OF_CANCELLED_POS] = flightsInfo1[NUMBER_OF_CANCELLED_POS] + flightsInfo1[IS_CANCELLED_POS] + flightsInfo2[NUMBER_OF_CANCELLED_POS] + flightsInfo2[IS_CANCELLED_POS];
            flightsInfo1[NUMBER_OF_DELAYED_POS] = flightsInfo1[NUMBER_OF_DELAYED_POS] + flightsInfo2[NUMBER_OF_DELAYED_POS];
            if (flightsInfo1[MAX_DELAY_POS] <= flightsInfo2[MAX_DELAY_POS]) {
                flightsInfo1[MAX_DELAY_POS] = flightsInfo2[MAX_DELAY_POS];
            }
            flightsInfo1[NUMBER_OF_FLIGHTS_POS] = flightsInfo1[NUMBER_OF_FLIGHTS_POS] + flightsInfo2[NUMBER_OF_FLIGHTS_POS];
            return flightsInfo1;
        });
    }

    private static JavaPairRDD<Pair<Integer, Integer>, String> dataToString(JavaPairRDD<Pair<Integer, Integer>, float[]> flights) {
        return flights.mapValues(arr -> "Max delay = " + arr[MAX_DELAY_POS] + "; Percent of delayes = " + arr[NUMBER_OF_DELAYED_POS] / arr[NUMBER_OF_FLIGHTS_POS] * 100 +
        "%; Percent of cancelled = " + arr[NUMBER_OF_CANCELLED_POS] / arr[NUMBER_OF_FLIGHTS_POS] * 100 + "%; Number of flights = " + arr[NUMBER_OF_FLIGHTS_POS]);
    }
}

