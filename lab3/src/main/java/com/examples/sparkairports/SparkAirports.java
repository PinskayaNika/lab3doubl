package com.examples.sparkairports;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class SparkAirports {
    public static void main(String[] args) throws Exception {
        //Инициализация приложения
        SparkConf conf = new SparkConf().setAppName("lab3doubl");
        JavaSparkContext sc = new JavaSparkContext(conf);


        //Загрузка данных
        JavaRDD<String> delayFile = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<String> airportFile = sc.textFile("L_AIRPORT_ID.csv");


        //Разбиение строки на слова
        JavaRDD<String> splittedDelay = delayFile.flatMap(s -> Arrays.stream(s.split(" ")).iterator());
        JavaRDD<String> splittedAirport = airportFile.flatMap(s -> Arrays.stream(s.split(" ")).iterator());

        //Отображение слов в пару <Слово,1>
        JavaPairRDD<String, Long> wordWithCountDelay = splittedDelay.mapToPair(s -> new Tuple2<>(s, 1)|);
        JavaPairRDD<String, Long> wordWithCountAirport = splittedAirport.mapToPair(s -> new Tuple2<>(s, 1|));


        //JavaSparkContext.textFile


       // final Broadcast<Map<String, AirportData>> airportsBroadcasted =
         //       sc.broadcast(stringAirportDataMap);
    }
}
