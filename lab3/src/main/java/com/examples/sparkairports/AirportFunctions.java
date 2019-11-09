package com.examples.sparkairports;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

class AirportFunctions {
    private static final int AIRPORT_ID_POS = 0;
    private static final int AIRPORT_NAME_POS = 1;

    static Broadcast<Map<Integer, String>> getAirportBroadcasted(JavaSparkContext sc, JavaRDD<String> airport) {
        //Преобразуем RDD в RDD пару ключ значение с помощью метода mapToPair
        //В качестве ключа для пары аэропортов используем класс Tuple2
        JavaPairRDD<Integer, String> airportPair = airport.mapToPair(s -> new Tuple2<>(
                Integer.parseInt(getAirportData(AIRPORT_ID_POS, s, true)),
                getAirportData(AIRPORT_NAME_POS, s, true))
        );
        //Для связывания с таблицей аэропортов — предварительно
        //выкачиваем список аэропортов в главную функцию с помощью метода collectAsMap
        Map<Integer, String> airportsMap = airportPair.collectAsMap();
                return sc.broadcast(airportsMap);

    }

    static String getAirportData(int pos, String s, boolean isAirport) {
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
