package by.sanko.joiner.entity;

import com.google.common.base.Objects;

import java.util.Date;

public class WeatherData {
    private Double longitude;
    private Double latitude;
    private Double avgTemprF;
    private Double avgTemprC;
    private Date weatherDate;
    private String geoHash;

    public WeatherData( Double longitude,Double latitude, Double avgTemprF, Double avgTemprC, Date weatherDate, String geoHash) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.avgTemprF = avgTemprF;
        this.avgTemprC = avgTemprC;
        this.weatherDate = weatherDate;
        this.geoHash = geoHash;
    }

    public Double getLatitude() {
        return latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public Double getAvgTemprF() {
        return avgTemprF;
    }

    public Double getAvgTemprC() {
        return avgTemprC;
    }

    public Date getWeatherDate() {
        return weatherDate;
    }

    public String getGeoHash() {
        return geoHash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WeatherData)) return false;
        WeatherData that = (WeatherData) o;
        return Objects.equal(latitude, that.latitude) && Objects.equal(longitude, that.longitude) &&
                Objects.equal(avgTemprF, that.avgTemprF) && Objects.equal(avgTemprC, that.avgTemprC)
                && Objects.equal(weatherDate, that.weatherDate) && Objects.equal(geoHash, that.geoHash);
    }

    @Override
    public int hashCode() {
        return  geoHash.hashCode();
    }

    @Override
    public String toString() {
        return "WeatherData{" +
                "latitude=" + latitude +
                ", longitude=" + longitude +
                ", avg_tempr_f=" + avgTemprF +
                ", avg_tempr_c=" + avgTemprC +
                ", weatherDate=" + weatherDate +
                ", geoHash='" + geoHash + '\'' +
                '}';
    }
}
