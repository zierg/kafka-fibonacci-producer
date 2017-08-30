package test.homework.kafka;

import homework.kafka.FibonacciProducer;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

@FieldDefaults(level = AccessLevel.PRIVATE)
public class FibonacciProducerTest {

    @Test
    public void test() throws IOException, TimeoutException {
        new FibonacciProducer(embeddedKafka.getBrokersAsString(), EXPECTED_AMOUNT).send();
        List<Integer> result = collectValues();
        assertThat(result).containsExactly(EXCEPTED);
    }
    private List<Integer> collectValues() {
        Map<String, Object> consumerProps = getConsumerProperties();
        KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(TOPIC));

        List<Integer> values = new ArrayList<>();
        long before = System.currentTimeMillis();
        while (System.currentTimeMillis() - before < TIME_LIMIT && values.size() < EXPECTED_AMOUNT) {
            ConsumerRecords<String, Integer> records = consumer.poll(100);
            for (val record : records) {
                values.add(record.value());
            }
        }
        return values;
    }

    private Map<String, Object> getConsumerProperties() {
        Map<String, Object> consumerProps =
                KafkaTestUtils.consumerProps("fibonacciConsumer", "false", embeddedKafka);
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        return consumerProps;
    }


    static final Integer[] EXCEPTED = {1, 1, 2, 3, 5, 8, 13, 21, 34, 55};
    static final int EXPECTED_AMOUNT = EXCEPTED.length;
    static final int TIME_LIMIT = 5000;
    static final String TOPIC = "fibonacci";

    @ClassRule
    public static final KafkaEmbedded embeddedKafka = new KafkaEmbedded(2, true, 1, TOPIC);
}