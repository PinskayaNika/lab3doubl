package com.examples.sparkairports;

import javafx.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Int;
import scala.Tuple2;

import java.sql.SQLTransactionRollbackException;
import java.util.Arrays;
import java.util.Map;

public class SparkAirports {
    private static final int AIRPORT_ID_POS = 0;
    private static final int AIRPORT_NAME_POS = 1;


    private static JavaRDD<String> mapAirportsID(JavaPairRDD<Pair<Integer, Integer>, String> flights, final Broadcast<Map<Integer, String>> airportsBroadcasted) {
        return flights.map(data -> {
            int airportID1 = data._1.getKey();
            int airportID2 = data._1.getValue();
            String info = data._2;
            String airportName1 = airportsBroadcasted.getValue().get(airportID1);
            String airportName2 = airportsBroadcasted.getValue().get(airportID2);
            info = airportID1 + "(" + airportName1 + ") -> " + airportID2 + "(" +airportName2 + ") " + info;
            return info;
        });
    }

    public static void main(String[] args) throws Exception {
        //Инициализация приложения
        SparkConf conf = new SparkConf().setAppName("lab3doubl");
        JavaSparkContext sc = new JavaSparkContext(conf);


        //Загрузка данных
        JavaRDD<String> flightFile = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<String> airportFile = sc.textFile("L_AIRPORT_ID.csv");


        //Разбиение строки на слова
        //JavaRDD<String> splittedDelay = flightFile.flatMap(s -> Arrays.stream(s.split(" ")).iterator());
        JavaRDD<String> splittedAirport = airportFile.flatMap(s -> Arrays.stream(s.split(" ")).iterator());

        //Отображение слов в пару <Слово,1>
       // JavaPairRDD<Integer, String> wordWithCountDelay = splittedDelay.mapToPair(s -> new Tuple2<>(s, 1)|);
        JavaPairRDD<Integer, String> wordWithCountAirport = splittedAirport.mapToPair(s -> new Tuple2<>(Integer.parseInt(AirportFunctions.getAirportData(AIRPORT_ID_POS, s, true)), AirportFunctions.getAirportData(AIRPORT_NAME_POS, s, true)));

        //JavaSparkContext.textFile


       // final Broadcast<Map<Integer, String>> airportsBroadcasted =
       //            AirportFunctions.getAirportBroadcasted(sc, splittedAirport);
//        final Broadcast<Map<Integer, String>> airportsBroadcasted =
//                AirportFunctions.getAirportBroadcasted(sc, splittedAirport);
        final Broadcast<Map<Integer, String>> airportsBroadcasted =
                sc.broadcast(wordWithCountAirport.collectAsMap());

        JavaPairRDD<Pair<Integer, Integer>, String> flightsHandler = FlightFunctions.handleFlight(flightFile);
        JavaRDD<String> output = mapAirportsID(flightsHandler, airportsBroadcasted);
        output.saveAsTextFile(args[0]);
    }
}
