package com.examples.sparkairports;

public class FlightFunctions {
    public static String getFromFlight(int pos, String s) {
        return s.split(",")[pos];
    }

}
