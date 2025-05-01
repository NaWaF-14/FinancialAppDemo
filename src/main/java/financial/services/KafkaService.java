package financial.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

public class KafkaService {
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper;
    private final String bootstrapServers;

    public KafkaService(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;

        Properties pProps = new Properties();
        pProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        pProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(pProps);

        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public <T> void produce(String topic, String key, T value) {
        try {
            String json = mapper.writeValueAsString(value);
            producer.send(new ProducerRecord<>(topic, key, json), (meta, err) -> {
                if (err != null) err.printStackTrace();
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize/produce", e);
        }
    }

    public <T> void consume(
            String topic,
            String groupId,
            Class<T> clazz,
            Consumer<T> handler
    ) {
        Thread thread = new Thread(() -> {
            Properties cProps = new Properties();
            cProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            cProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            cProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            cProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            cProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            try (KafkaConsumer<String,String> consumer = new KafkaConsumer<>(cProps)) {
                consumer.subscribe(List.of(topic));
                while (true) {
                    var records = consumer.poll(Duration.ofMillis(200));
                    for (var rec : records) {
                        T obj = mapper.readValue(rec.value(), clazz);
                        handler.accept(obj);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "kafka-consumer-" + topic + "-" + groupId);

        thread.setDaemon(true);
        thread.start();
    }

    public void close() {
        producer.close();
    }
}
