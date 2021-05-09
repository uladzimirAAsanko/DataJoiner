package by.sanko.joiner.main;

import by.sanko.joiner.entity.HotelData;
import by.sanko.joiner.parser.HotelParser;
import by.sanko.joiner.parser.WeatherParser;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import javafx.util.Pair;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    private static final String CONNECTION = "host.docker.internal:9094";
    private static final String CONSUMER_GROUP = "KafkaExampleConsumer";
    private static final String SUBSCRIBE_TOPIC_HOTEL = "hw-data-topic";
    private static final String SUBSCRIBE_TOPIC_WEATHER = "weather-data-hash";
    private static final String OUTPUT_TOPIC = "weather-data-hash";

    static Consumer<String, String> consumerHotel = null;
    static Consumer<String, String> consumerWeather = null;
    static Producer<String, String> producer = null;

    public static void main(String[] args) {
        init();
        consumerWeather.poll(0);
        consumerWeather.seekToBeginning(consumerWeather.assignment());
        System.out.println("Started to read weather data from topic " + SUBSCRIBE_TOPIC_WEATHER);
        AtomicInteger i = new AtomicInteger();
        while (true) {
            final ConsumerRecords<String, String> consumerRecords = consumerWeather.poll(1000);
            if (consumerRecords.count() == 0) {
                break;
            }
            consumerRecords.forEach(record -> {
                if(i.get() % 1000 == 0){
                    System.out.println(i.get()/1000 + "th record is :");
                    System.out.println(WeatherParser.parseData(record.value()).toString());
                }
                i.getAndIncrement();
            });
            consumerWeather.commitAsync();
        }
        consumerWeather.close();
        System.out.println("DONE");
        System.out.println("All rows are " + i);

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
        consumerHotel.subscribe(Collections.singletonList(SUBSCRIBE_TOPIC_WEATHER));
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
