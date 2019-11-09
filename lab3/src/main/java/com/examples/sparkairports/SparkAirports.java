package com.examples.sparkairports;
//
//import javafx.util.Pair;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.broadcast.Broadcast;
//import scala.Int;
//import scala.Tuple2;
//
//import java.sql.SQLTransactionRollbackException;
//import java.util.Arrays;
//import java.util.Map;
//
//public class SparkAirports {
//    private static final int AIRPORT_ID_POS = 0;
//    private static final int AIRPORT_NAME_POS = 1;
//
//
//    private static JavaRDD<String> mapAirportsID(JavaPairRDD<Pair<Integer, Integer>, String> flights, final Broadcast<Map<Integer, String>> airportsBroadcasted) {
//        return flights.map(data -> {
//            int airportID1 = data._1.getKey();
//            int airportID2 = data._1.getValue();
//            String info = data._2;
//            String airportName1 = airportsBroadcasted.getValue().get(airportID1);
//            String airportName2 = airportsBroadcasted.getValue().get(airportID2);
//            info = airportID1 + "(" + airportName1 + ") -> " + airportID2 + "(" +airportName2 + ") " + info;
//            return info;
//        });
//    }
//
//    private static JavaRDD<String> getDataFromFile(JavaSparkContext sc, String path) {
//        JavaRDD<String> data = sc.textFile(path).flatMap(s -> Arrays.stream(s.split(" ")).iterator());;
//        final String header = data.first();
//        return data.filter(line -> !line.equals(header));
//    }
//
//    public static void main(String[] args) throws Exception {
//        //Инициализация приложения
//        SparkConf conf = new SparkConf().setAppName("lab3doubl");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//
//        //Загрузка данных
//        //JavaRDD<String> flightFile = sc.textFile("664600583_T_ONTIME_sample.csv");
//        //JavaRDD<String> airportFile = sc.textFile("L_AIRPORT_ID.csv");
//
//
//        //Разбиение строки на слова
//        //JavaRDD<String> splittedDelay = flightFile.flatMap(s -> Arrays.stream(s.split(" ")).iterator());
//        //JavaRDD<String> splittedAirport = airportFile.flatMap(s -> Arrays.stream(s.split(" ")).iterator());
//
//        //Отображение слов в пару <Слово,1>
//       // JavaPairRDD<Integer, String> wordWithCountDelay = splittedDelay.mapToPair(s -> new Tuple2<>(s, 1)|);
//
//        //JavaSparkContext.textFile
//
//
//       // final Broadcast<Map<Integer, String>> airportsBroadcasted =
//       //            AirportFunctions.getAirportBroadcasted(sc, splittedAirport);
////        final Broadcast<Map<Integer, String>> airportsBroadcasted =
////                AirportFunctions.getAirportBroadcasted(sc, splittedAirport);
//
//        JavaRDD<String> flightFile = getDataFromFile(sc, "664600583_T_ONTIME_sample.csv");
//        JavaRDD<String> airportFile = getDataFromFile(sc, "L_AIRPORT_ID.csv");
//
////        JavaPairRDD<Integer, String> wordWithCountAirport = airportFile.mapToPair(s -> new Tuple2<>(Integer.parseInt(AirportFunctions.getAirportData(AIRPORT_ID_POS, s, true)), AirportFunctions.getAirportData(AIRPORT_NAME_POS, s, true)));
////
////        final Broadcast<Map<Integer, String>> airportsBroadcasted =
////                sc.broadcast(wordWithCountAirport.collectAsMap());
////
//
//        final Broadcast<Map<Integer,String>> airportsBroadcasted = AirportFunctions.getAirportBroadcasted(sc,airportFile);
//        JavaPairRDD<Pair<Integer, Integer>, String> flightsHandler = FlightFunctions.handleFlight(flightFile);
//        JavaRDD<String> output31 = mapAirportsID(flightsHandler, airportsBroadcasted);
//        output31.saveAsTextFile(args[0]);
//    }
//}


import javafx.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.*;

public class SparkAirports {

    private static final String AIRPORT_ID_CSV = "L_AIRPORT_ID.csv";
    private static final String FLIGHT_CSV = "664600583_T_ONTIME_sample.csv";

    private static JavaRDD<String> getDataFromFile(JavaSparkContext sc, String path){
        //Загрузка данных
        //Разбиение строки на слова
        //.textFile – загружает файл из hdfs. Каждая запись RDD – строка.
        //FlatMap – отображение элемента RDD в несколько элементов
        JavaRDD<String> data = sc.textFile(path).flatMap(s -> Arrays.stream(s.split("\t")).iterator());
        final String header = data.first();
        //Filter – фильтрация записей RDD
        return data.filter(line -> !line.equals(header));  //пропускаем первую строку заголовков
    }

//в методе map преобразуем итоговый RDD содержащий статистические
//данные — обогащаем его именами аэропортов, обращаясь внутри
//функций к объекту airportsBroadcasted.value()
    private static JavaRDD<String> mapAirportsIDs(JavaPairRDD<Pair<Integer, Integer>, String> schedule, final Broadcast<Map<Integer,String>> airportsBroadcasted){
        return schedule.map(data -> {
            int airportID1 = data._1.getKey();
            int airportID2 = data._1.getValue();
            String info = data._2;
            String airportName1 = airportsBroadcasted.getValue().get(airportID1);
            String airportName2 = airportsBroadcasted.getValue().get(airportID2);
            info = airportID1 + " (" + airportName1 + ") -> " + airportID2 + " (" + airportName2 + ") " + info;
            return info;
        });
    }


    public static void main(String[] args){
        //Инициализация приложения
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> airportFile = getDataFromFile(sc, AIRPORT_ID_CSV);
        JavaRDD<String> flightFile = getDataFromFile(sc, FLIGHT_CSV);
        //Создаем в основном методе main переменную broadcast
        final Broadcast<Map<Integer,String>> airportsBroadcasted = AirportFunctions.getAirportBroadcasted(sc,airportFile);
        JavaPairRDD<Pair<Integer, Integer>, String> flightHandler = FlightFunctions.handleFlight(flightFile);
        JavaRDD<String> output = mapAirportsIDs(flightHandler, airportsBroadcasted);
        output.saveAsTextFile(args[0]);
    }
}

