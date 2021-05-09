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
            residue = residue.substring(indexOfComma + 1);
        }
        System.out.println(residue);
        list.add(residue);
        long id = Long.parseLong(list.get(0));
        String name = list.get(1);
        String country = list.get(2);
        String city = list.get(3);
        String address = list.get(4);
        double lng = 0;
        String geoHash = list.get(7);
        double lat = 0;
        try {
            lat = Double.parseDouble(list.get(6));
            lng = Double.parseDouble(list.get(5));
        }catch (NumberFormatException e){
            geoHash = data.substring(data.lastIndexOf(comma) + 1);
            System.out.println(geoHash);
            data = data.substring(0, data.lastIndexOf(comma));
            lat = Double.parseDouble(data.substring(data.lastIndexOf(comma) + 1));
            System.out.println(lat);
            data = data.substring(0, data.lastIndexOf(comma));
            lng = Double.parseDouble(data.substring(data.lastIndexOf(comma) + 1));
            System.out.println(lng);
            data = data.substring(0, data.lastIndexOf(comma));
            address = address + data.substring(data.lastIndexOf(comma) + 1);
            System.out.println(address);
        }
        return new HotelData(id, name, country, city ,address, lng, lat, geoHash);
    }
}
