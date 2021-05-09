package by.sanko.joiner.parser;

import by.sanko.joiner.entity.HotelData;
import by.sanko.joiner.entity.WeatherData;
import jdk.nashorn.internal.parser.DateParser;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class WeatherParser {
    private static final char comma = ',';

    public static WeatherData parseData(String data){
        List<String> list = Parser.parse(data, 6);
        Double lng = Double.parseDouble(list.get(0));
        Double lat = Double.parseDouble(list.get(1));
        Double tempF = Double.parseDouble(list.get(2));
        Double tempC = Double.parseDouble(list.get(3));
        Date date = null;
        try {
            date = new SimpleDateFormat("yyyy-M-d").parse(list.get(4));
        } catch (ParseException e) {
            System.out.println("Error while parsing " + list.get(4));
        }
        String geoHash = list.get(5);
        return new WeatherData(lng, lat, tempF, tempC, date, geoHash);
    }
}
