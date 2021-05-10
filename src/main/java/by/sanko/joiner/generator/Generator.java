package by.sanko.joiner.generator;

import ch.hsr.geohash.GeoHash;

public class Generator {
    public static String generateGeoHash(Double lat, Double lng){
        GeoHash geoHash = GeoHash.withCharacterPrecision(lat, lng, 4);
        String geoHashString = geoHash.toBase32();
        return geoHashString;
    }
}
