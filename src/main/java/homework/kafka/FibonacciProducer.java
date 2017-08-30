package homework.kafka;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class FibonacciProducer {

    public FibonacciProducer(String servers, int amount) {
        this.props = initProps(servers);
        this.amount = amount;
    }

    public void send() {
        log.info("Start sending fibonacci numbers. Amount: {}", amount);
        Producer<String, Long> producer = new KafkaProducer<>(props);
        long previous1 = 0;
        long previous2 = 1;
        for(int i = 0; i < amount; i++) {
            log.info("Sending the {}-th number: {}", i, previous2);
            producer.send(new ProducerRecord<>(TOPIC, previous2));
            long newNumber = previous1 + previous2;
            previous1 = previous2;
            previous2 = newNumber;
        }

        producer.close();
    }

    private Properties initProps(String servers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        return props;
    }

    int amount;
    Properties props;

    static String TOPIC = "fibonacci";

    static Logger log = LoggerFactory.getLogger(FibonacciProducer.class);
}