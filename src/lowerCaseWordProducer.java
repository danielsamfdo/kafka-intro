import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class lowerCaseWordProducer {
    public static void main(String args[]) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        String topic = "lowerCase";
        for (int i = 0; i < 10; i++) {
            String key = String.format("Key-%d",i);
            String message = String.format("LowerCase-%s", key);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null) {
                        String x = String.format("Received MD \n Topic: %s \n Partition: %d \n Offset: %s \n TimeStamp: %s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                        System.out.println(x);
                    }
                    else {
                        System.out.println("error while producing");
                    }
                }
            });

            producer.flush();
        }


        producer.close();
    }
}
