package by.sanko.joiner.parser;

import by.sanko.joiner.entity.HotelData;

import java.util.ArrayList;
import java.util.List;

public class HotelParser {
    private static final char comma = ',';

    public static HotelData parseData(String data){
        List<String> list = new ArrayList<>();
        int indexOfComma = data.indexOf(comma);
        list.add(data.substring(0, indexOfComma));
        for(int i = 0; i < 6; i++){
            int indexOf2nd = data.indexOf(indexOfComma + 1, comma);
            list.add(data.substring(indexOfComma +1, indexOf2nd));
            indexOfComma = indexOf2nd;
        }
        list.add(data.substring(indexOfComma + 1));
        Long id = Long.parseLong(list.get(0));
        String name = list.get(1);
        String country = list.get(2);
        String city = list.get(3);
        String address = list.get(4);
        Double lng = Double.parseDouble(list.get(5));
        Double lat = Double.parseDouble(list.get(6));
        String geoHash = list.get(7);
        return new HotelData(id, name, country, city ,address, lng, lat, geoHash);
    }
}
