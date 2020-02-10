import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class upperCaseWordProducer {
    public static void main(String args[]) {
        Properties props = new Properties();
        String topic = "lowerCase";
        String groupId = "grp-1";
        String bootstrapServers = "localhost:9092";
        String ptopic="upperCase";
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String, String> record: records) {
                System.out.printf("\n Key %s\n Value %s\n Partition %s\n Offset: %s\n",
                        record.key(), record.value(), record.partition(), record.offset());
                ProducerRecord<String, String> precord = new ProducerRecord<>(ptopic, record.key(), record.value().toUpperCase());
                producer.send(precord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e == null) {
                            String x =String.format("Received MD \n Topic: %s \n Partition: %d \n Offset: %s \n TimeStamp: %s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                            System.out.println(x);
                        }
                        else {
                            System.out.println("error while producing");
                        }
                    }
                });
            }
        }
    }
}
