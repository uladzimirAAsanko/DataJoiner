package by.sanko.joiner.main;

import by.sanko.joiner.entity.HotelData;
import by.sanko.joiner.entity.WeatherData;
import by.sanko.joiner.parser.HotelParser;
import by.sanko.joiner.parser.WeatherParser;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    private static final String CONNECTION = "host.docker.internal:9094";
    private static final String CONSUMER_GROUP = "KafkaExampleConsumer";
    private static final String SUBSCRIBE_TOPIC_HOTEL = "hw-data-topic";
    private static final String SUBSCRIBE_TOPIC_WEATHER = "weathers-data-hash";
    private static final String OUTPUT_TOPIC = "weather-data-hash";

    static Consumer<String, String> consumerHotel = null;
    static Consumer<String, String> consumerWeather = null;
    static Producer<String, String> producer = null;

    public static void main(String[] args) throws ParseException {
        init();
        List<HotelData> hotels = readHotels();
        HashSet<String> geoHashes = new HashSet<>();
        for(HotelData hotel : hotels){
            geoHashes.add(hotel.getGeoHash());
            System.out.println(hotel.getGeoHash());
        }
        HashMap<Date,HashMap<String, Pair<Double, Integer>>> listOfMaps =  new HashMap<>();
        for(int i = 1; i < 32; i++){
            Date date = new SimpleDateFormat("yyyy-M-d").parse("2016-10-"+i);
            HashMap<String, Pair<Double, Integer>> map = new HashMap<>();
            for(String geoHash : geoHashes){
                map.put(geoHash, new MutablePair<Double, Integer>(0.0, 0));
            }
            listOfMaps.put(date,map);
        }

        for(int i = 1; i < 31; i++){
            Date date = new SimpleDateFormat("yyyy-M-d").parse("2017-09-"+i);
            HashMap<String, Pair<Double, Integer>> map = new HashMap<>();
            for(String geoHash : geoHashes){
                map.put(geoHash, new MutablePair<Double, Integer>(0.0, 0));
            }
            listOfMaps.put(date,map);
        }

        for(int i = 1; i < 32; i++){
            Date date = new SimpleDateFormat("yyyy-M-d").parse("2017-08-"+i);
            HashMap<String, Pair<Double, Integer>> map = new HashMap<>();
            for(String geoHash : geoHashes){
                map.put(geoHash, new MutablePair<Double, Integer>(0.0, 0));
            }
            listOfMaps.put(date,map);
        }
        consumerWeather.poll(0);
        consumerWeather.seekToBeginning(consumerWeather.assignment());
        System.out.println("Started to read weather data from topic " + SUBSCRIBE_TOPIC_WEATHER);
        while (true) {
            final ConsumerRecords<String, String> consumerRecords = consumerWeather.poll(1000);
            if (consumerRecords.count() == 0) {
                break;
            }
            consumerRecords.forEach(record -> {
                String value = record.value();
                WeatherData data = WeatherParser.parseData(value);
                System.out.println(data.toString());
                HashMap<String, Pair<Double, Integer>> map =  listOfMaps.get(data.getWeatherDate());
                System.out.println(map.keySet());
                Pair<Double,Integer> pair = map.get(data.getGeoHash());
                if(pair != null) {
                    Double avg_temp = (Double) pair.getLeft();
                    Integer count = (Integer) pair.getRight();
                    System.out.println("Old value are " + avg_temp + "  " + count);
                    count += 1;
                    avg_temp += data.getAvgTemprC();
                    Pair<Double, Integer> changed = new MutablePair<Double, Integer>(avg_temp, count);
                    System.out.println("New value are " + avg_temp + "  " + count);
                    map.replace(data.getGeoHash(), pair, changed);
                }
            });
            consumerWeather.commitAsync();
        }
        consumerWeather.close();
        System.out.println("DONE");

    }

    private static void init(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", CONNECTION);
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer = new KafkaProducer<>(properties);

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CONNECTION);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerHotel = new KafkaConsumer<>(props);
        consumerWeather = new KafkaConsumer<>(props);
        consumerHotel.subscribe(Collections.singletonList(SUBSCRIBE_TOPIC_HOTEL));
        consumerWeather.subscribe(Collections.singletonList(SUBSCRIBE_TOPIC_WEATHER));
    }

    private static List<HotelData> readHotels(){
        System.out.println("Started to read Hotel data from topic " + SUBSCRIBE_TOPIC_HOTEL);
        List<String> hotels = new ArrayList<>();
        consumerHotel.poll(0);
        consumerHotel.seekToBeginning(consumerHotel.assignment());
        while (true) {
            final ConsumerRecords<String, String> consumerRecords = consumerHotel.poll(1000);
            if (consumerRecords.count() == 0) {
                break;
            }
            consumerRecords.forEach(record -> {
                String value = record.value();
                int index = value.indexOf('\n');
                hotels.add(value.substring(index + 1, value.indexOf('\n', index +1)));
            });
            consumerHotel.commitAsync();
            System.out.println("All rows are " + hotels.size());
        }
        System.out.println("DONE");
        consumerHotel.close();
        System.out.println("All rows are " + hotels.size());
        System.out.println("First row is " + hotels.get(0));
        System.out.println("Last row is " + hotels.get(hotels.size() - 1));
        List<HotelData> hotelData = new ArrayList<>();
        for(String hotel : hotels){
            HotelData data = HotelParser.parseData(hotel);
            hotelData.add(data);
        }
        System.out.println("All hotels count " + hotelData.size());
        return hotelData;
    }
}
