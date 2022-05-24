package demo.kafka.streams.serdes;

import demo.kafka.streams.event.PaymentEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/**
 * Requires the WrapperSerdes to allow this to be added as the default serdes config in the KafkaStreams configuration.
 */
public final class PaymentSerdes extends Serdes.WrapperSerde<PaymentEvent> {

    public PaymentSerdes() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(PaymentEvent.class));
    }

    public static Serde<PaymentEvent> serdes() {
        JsonSerializer<PaymentEvent> serializer = new JsonSerializer<>();
        JsonDeserializer<PaymentEvent> deserializer = new JsonDeserializer<>(PaymentEvent.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
