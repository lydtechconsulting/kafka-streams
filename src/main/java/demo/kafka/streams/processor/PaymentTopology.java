package demo.kafka.streams.processor;

import java.util.Arrays;
import java.util.List;

import demo.kafka.streams.event.PaymentEvent;
import demo.kafka.streams.properties.KafkaStreamsDemoProperties;
import demo.kafka.streams.serdes.PaymentSerdes;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class PaymentTopology {

    @Autowired
    private final KafkaStreamsDemoProperties properties;

    private static List SUPPORTED_RAILS = Arrays.asList(Rails.BANK_RAILS_FOO.name(), Rails.BANK_RAILS_BAR.name());

    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Long> LONG_SERDE = Serdes.Long();

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {

        KStream<String, PaymentEvent> messageStream = streamsBuilder
            .stream(properties.getPaymentInboundTopic(), Consumed.with(STRING_SERDE, PaymentSerdes.serdes()))
            .peek((key, payment) -> log.info("Payment event received with key=" + key + ", payment=" + payment))

            // Filter out unsupported bank rails.
            .filter((key, payment) -> SUPPORTED_RAILS.contains(payment.getRails()))
            .peek((key, value) -> log.info("Filtered payment event received with key=" + key + ", value=" + value));

        // Branch based on currency in order to perform any FX.
        KStream<String, PaymentEvent>[] currenciesBranches = messageStream.branch(
            (key, payment) -> payment.getCurrency().equals(Currency.GBP.name()),
            (key, payment) -> payment.getCurrency().equals(Currency.USD.name())
        );
        KStream<String, PaymentEvent> fxStream = currenciesBranches[1].mapValues(
            // Use mapValues() as we are transforming the payment, but not changing the key.
            (payment) -> {
                // Perform FX conversion.
                double usdToGbpRate = 0.8;
                PaymentEvent transformedPayment = PaymentEvent.builder()
                        .paymentId(payment.getPaymentId())
                        .amount(Math.round(payment.getAmount() * usdToGbpRate))
                        .currency(Currency.GBP.name())
                        .fromAccount(payment.getFromAccount())
                        .toAccount(payment.getToAccount())
                        .rails(payment.getRails())
                        .build();
                return transformedPayment;
            });

        // Merge the payment streams back together.
        KStream<String, PaymentEvent> mergedStreams = currenciesBranches[0].merge(fxStream)
            .peek((key, value) -> log.info("Merged payment event received with key=" + key + ", value=" + value));

        // Create the KTable stateful store to track account balances.
        mergedStreams
            .map((key, payment) -> new KeyValue<>(payment.getFromAccount(), payment.getAmount()))
            .groupByKey(Grouped.with(STRING_SERDE, LONG_SERDE))
            .aggregate(new Initializer<Long>() {
                @Override
                public Long apply() {
                    return 0L;
                }
            }, new Aggregator<String, Long, Long>() {
                @Override
                public Long apply(final String key, final Long value, final Long aggregate) {
                    return aggregate + value;
                }
            }, Materialized.with(STRING_SERDE, LONG_SERDE).as("balance"));

        // Branch based on bank rails for outbound publish.
        KStream<String, PaymentEvent>[] railsBranches = mergedStreams.branch(
            (key, payment) -> payment.getRails().equals(Rails.BANK_RAILS_FOO.name()),
            (key, payment) -> payment.getRails().equals(Rails.BANK_RAILS_BAR.name()));

        // Publish outbound events.
        railsBranches[0].to(properties.getRailsFooOutboundTopic(), Produced.with(STRING_SERDE, PaymentSerdes.serdes()));
        railsBranches[1].to(properties.getRailsBarOutboundTopic(), Produced.with(STRING_SERDE, PaymentSerdes.serdes()));
    }
}
