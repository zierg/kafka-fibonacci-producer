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
        List<Long> result = collectValues();
        assertThat(result).containsExactly(EXCEPTED);
    }
    private List<Long> collectValues() {
        Map<String, Object> consumerProps = getConsumerProperties();
        KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(TOPIC));

        List<Long> values = new ArrayList<>();
        long before = System.currentTimeMillis();
        while (System.currentTimeMillis() - before < TIME_LIMIT && values.size() < EXPECTED_AMOUNT) {
            ConsumerRecords<String, Long> records = consumer.poll(100);
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
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        return consumerProps;
    }


    static final Long[] EXCEPTED = {1L, 1L, 2L, 3L, 5L, 8L, 13L, 21L, 34L, 55L};
    static final int EXPECTED_AMOUNT = EXCEPTED.length;
    static final int TIME_LIMIT = 5000;
    static final String TOPIC = "fibonacci";

    @ClassRule
    public static final KafkaEmbedded embeddedKafka = new KafkaEmbedded(2, true, 1, TOPIC);
}