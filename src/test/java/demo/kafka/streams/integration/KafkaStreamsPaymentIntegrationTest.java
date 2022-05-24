package demo.kafka.streams.integration;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import demo.kafka.streams.KafkaStreamsDemoConfiguration;
import demo.kafka.streams.event.PaymentEvent;
import demo.kafka.streams.mapper.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import static demo.kafka.streams.processor.Rails.BANK_RAILS_BAR;
import static demo.kafka.streams.processor.Rails.BANK_RAILS_FOO;
import static demo.kafka.streams.processor.Rails.BANK_RAILS_XXX;
import static demo.kafka.streams.util.TestEventData.buildPaymentEvent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@Slf4j
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = { KafkaStreamsDemoConfiguration.class } )
@EmbeddedKafka(controlledShutdown = true, topics = { "payment-topic", "rails-foo-topic", "rails-bar-topic" })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
public class KafkaStreamsPaymentIntegrationTest {

    private final static String PAYMENT_TEST_TOPIC = "payment-topic";

    @Autowired
    private KafkaTemplate testKafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private KafkaFooRailsListener fooRailsReceiver;

    @Autowired
    private KafkaBarRailsListener barRailsReceiver;

    // GBP Accounts.
    private static final String ACCOUNT_GBP_ABC = "ABC-"+UUID.randomUUID();
    private static final String ACCOUNT_GBP_DEF = "DEF-"+UUID.randomUUID();

    // USD Accounts.
    private static final String ACCOUNT_USD_XYZ = "XYZ-"+UUID.randomUUID();

    @Configuration
    static class TestConfig {

        @Bean
        public KafkaFooRailsListener fooRailsReceiver() {
            return new KafkaFooRailsListener();
        }

        @Bean
        public KafkaBarRailsListener barRailsReceiver() {
            return new KafkaBarRailsListener();
        }
    }

    public static class KafkaFooRailsListener {
        AtomicInteger counter = new AtomicInteger(0);
        AtomicLong total = new AtomicLong(0);

        @KafkaListener(groupId = "KafkaStreamsIntegrationTest", topics = "rails-foo-topic", autoStartup = "true")
        void receive(@Payload final String payload, @Headers final MessageHeaders headers) {
            log.debug("KafkaFooRailsListener - Received message: " + payload);
            PaymentEvent payment = JsonMapper.readFromJson(payload, PaymentEvent.class);
            total.addAndGet(payment.getAmount());
            counter.incrementAndGet();
        }
    }

    public static class KafkaBarRailsListener {
        AtomicInteger counter = new AtomicInteger(0);
        AtomicLong total = new AtomicLong(0);

        @KafkaListener(groupId = "KafkaStreamsIntegrationTest", topics = "rails-bar-topic", autoStartup = "true")
        void receive(@Payload final String payload, @Headers final MessageHeaders headers) {
            log.debug("KafkaBarRailsListener - Received message: " + payload);
            PaymentEvent payment = JsonMapper.readFromJson(payload, PaymentEvent.class);
            total.addAndGet(payment.getAmount());
            counter.incrementAndGet();
        }
    }

    @BeforeEach
    public void setUp() {
        // Wait until the partitions are assigned.
        registry.getListenerContainers().stream().forEach(container ->
                ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic()));

        fooRailsReceiver.counter.set(0);
        barRailsReceiver.counter.set(0);
    }

    /**
     * Send a number of payments to the inbound payments topic.
     *
     * They will be processed and outbound events sent to two different topics, based on the payment rails specified.
     *
     * This test has listeners for both the outbound topics, so the expected events can be asserted.
     *
     * The test then calls the balance endpoint to ensure the aggregated amounts are correct.
     */
    @Test
    public void testKafkaStreams() throws Exception {

        // Three payments via FOO rails from ABC to DEF, total 210 GBP.
        PaymentEvent payment1 = buildPaymentEvent(UUID.randomUUID().toString(),
                100L,
                "GBP",
                ACCOUNT_GBP_ABC,
                ACCOUNT_GBP_DEF,
                BANK_RAILS_FOO.name());
        sendMessage(PAYMENT_TEST_TOPIC, payment1);
        PaymentEvent payment2 = buildPaymentEvent(UUID.randomUUID().toString(),
                50L,
                "GBP",
                ACCOUNT_GBP_ABC,
                ACCOUNT_GBP_DEF,
                BANK_RAILS_FOO.name());
        sendMessage(PAYMENT_TEST_TOPIC, payment2);
        PaymentEvent payment3 = buildPaymentEvent(UUID.randomUUID().toString(),
                60L,
                "GBP",
                ACCOUNT_GBP_ABC,
                ACCOUNT_GBP_DEF,
                BANK_RAILS_FOO.name());
        sendMessage(PAYMENT_TEST_TOPIC, payment3);

        // Payment on an unsupported rails should be filtered out.
        PaymentEvent payment4 = buildPaymentEvent(UUID.randomUUID().toString(),
                1200L,
                "GBP",
                ACCOUNT_GBP_ABC,
                ACCOUNT_GBP_DEF,
                BANK_RAILS_XXX.name());
        sendMessage(PAYMENT_TEST_TOPIC, payment4);

        // Payment from a USD account will require FX.
        PaymentEvent payment5 = buildPaymentEvent(UUID.randomUUID().toString(),
                1000L,  // Converts to 800 GBP.
                "USD",
                ACCOUNT_USD_XYZ,
                ACCOUNT_GBP_DEF,
                BANK_RAILS_BAR.name());
        sendMessage(PAYMENT_TEST_TOPIC, payment5);

        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(fooRailsReceiver.counter::get, equalTo(3));
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(barRailsReceiver.counter::get, equalTo(1));

        // Assert total amounts received by the rails are as expected.
        assertThat(fooRailsReceiver.total.get(), equalTo(210L));
        assertThat(barRailsReceiver.total.get(), equalTo(800L));

        // Query the stateful balance stores for two accounts.
        ResponseEntity<String> responseAbc = restTemplate.getForEntity("/v1/kafka-streams/balance/"+ACCOUNT_GBP_ABC, String.class);
        assertThat(responseAbc.getStatusCode(), equalTo(HttpStatus.OK));
        assertThat(responseAbc.getBody(), equalTo("210")); // Payments: 100 + 60 + 50.

        ResponseEntity<String> responseDef = restTemplate.getForEntity("/v1/kafka-streams/balance/"+ACCOUNT_GBP_DEF, String.class);
        assertThat(responseDef.getStatusCode(), equalTo(HttpStatus.NOT_FOUND)); // No payments from this account.

        ResponseEntity<String> responseXyz = restTemplate.getForEntity("/v1/kafka-streams/balance/"+ACCOUNT_USD_XYZ, String.class);
        assertThat(responseXyz.getStatusCode(), equalTo(HttpStatus.OK));
        assertThat(responseXyz.getBody(), equalTo("800")); // 1000 USD * 0.8 FX.
    }

    /**
     * Test the topology description endpoint is working.
     *
     * Capture the topology body and use:
     * https://zz85.github.io/kafka-streams-viz/
     * to visualise the topology.
     */
    @Test
    public void testTopology() throws Exception {
        ResponseEntity<String> topology = restTemplate.getForEntity("/v1/kafka-streams/topology/", String.class);
        assertThat(topology.getStatusCode(), equalTo(HttpStatus.OK));
        assertThat(topology.getBody(), containsString("topics: [payment-topic]"));
        log.info(topology.getBody());
    }

    /**
     * Send the given payment event to the given topic.
     */
    private SendResult sendMessage(String topic, PaymentEvent event) throws Exception {
        String payload = JsonMapper.writeToJson(event);
        List<Header> headers = new ArrayList<>();
        final ProducerRecord<Long, String> record = new ProducerRecord(topic, null, event.getPaymentId(), payload, headers);

        final SendResult result = (SendResult)testKafkaTemplate.send(record).get();
        final RecordMetadata metadata = result.getRecordMetadata();

        log.debug(String.format("Sent record(key=%s value=%s) meta(topic=%s, partition=%d, offset=%d)",
                record.key(), record.value(), metadata.topic(), metadata.partition(), metadata.offset()));

        return result;
    }
}
