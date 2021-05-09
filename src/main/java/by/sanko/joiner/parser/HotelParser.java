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
        String residue = data.substring(indexOfComma + 1);
        for(int i = 0; i < 6; i++){
            indexOfComma = residue.indexOf(comma);
            String tmp = residue.substring(0, indexOfComma);
            System.out.println(tmp);
            list.add(residue.substring(0, indexOfComma));
            residue = data.substring(indexOfComma + 1);
        }
        System.out.println(residue);
        list.add(residue);
        long id = Long.parseLong(list.get(0));
        String name = list.get(1);
        String country = list.get(2);
        String city = list.get(3);
        String address = list.get(4);
        double lng = Double.parseDouble(list.get(5));
        double lat = Double.parseDouble(list.get(6));
        String geoHash = list.get(7);
        return new HotelData(id, name, country, city ,address, lng, lat, geoHash);
    }
}
