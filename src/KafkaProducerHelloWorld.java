
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

class KafkaProducerHelloWorld {

    public static void main(String args[]) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        String topic = "ipStream";
        for (int i = 0; i < 1000; i++) {
            String key = String.format("Key %d",i);
            String message = String.format("Message %s", key);
            /* Simple Way TO Call
            producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), Integer.toString(i)));
            */

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
            producer.send(record, new Callback() {
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


            /* Another way to call
            producer.send(record, (recordMetadata, e) -> {
                if(e == null) {
                    System.out.println("HIIII");
                    String x =String.format("Received MD \n Topic: %s \n Partition: %d \n Offset:  ", recordMetadata.topic(), recordMetadata.partition());
                    System.out.println(x);
                }
                else {
                    System.out.println("error while producing");
                }

            }).get(); // Synchronous
             */
            producer.flush();
        }

        // Hello World Snippet
        producer.send(new ProducerRecord<String, String>(topic, "Hello"));
        producer.send(new ProducerRecord<String, String>(topic, "World"));

        producer.close();
    }
}