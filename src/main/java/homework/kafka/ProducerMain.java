package homework.kafka;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class ProducerMain {

    public static void main(String[] args) {
        int amount = getAmount(args);
        new FibonacciProducer("localhost:9092", amount).send();
    }

    private static int getAmount(String[] args) {
        if (args.length == 0) {
            log.info("No Fibonacci amount has been provided. The default value {} will be used.", DEFAULT_AMOUNT);
            return DEFAULT_AMOUNT;
        }
        try {
            return Integer.parseInt(args[0]);
        } catch (NumberFormatException ex) {
            log.warn("The provided value '{}' is not a valid integer. The default value {} will be used.", args[0], DEFAULT_AMOUNT);
            return DEFAULT_AMOUNT;
        }
    }

    static int DEFAULT_AMOUNT = 10;

    static Logger log = LoggerFactory.getLogger(ProducerMain.class);
}
