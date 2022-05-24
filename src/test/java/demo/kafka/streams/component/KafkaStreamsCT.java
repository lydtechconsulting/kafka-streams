package demo.kafka.streams.component;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import demo.kafka.streams.event.PaymentEvent;
import dev.lydtech.component.framework.client.kafka.KafkaClient;
import dev.lydtech.component.framework.client.service.ServiceClient;
import dev.lydtech.component.framework.extension.TestContainersSetupExtension;
import dev.lydtech.component.framework.mapper.JsonMapper;
import io.restassured.RestAssured;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.test.context.ActiveProfiles;

import static demo.kafka.streams.processor.Currency.GBP;
import static demo.kafka.streams.processor.Currency.USD;
import static demo.kafka.streams.processor.Rails.BANK_RAILS_BAR;
import static demo.kafka.streams.processor.Rails.BANK_RAILS_FOO;
import static demo.kafka.streams.util.TestEventData.buildPaymentEvent;
import static io.restassured.RestAssured.get;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
@ExtendWith(TestContainersSetupExtension.class)
@ActiveProfiles("component-test")
public class KafkaStreamsCT {

    private final static String GROUP_ID = "KafkaStreamsComponentTest";
    private final static String PAYMENT_TOPIC = "payment-topic";
    private final static String RAILS_FOO_TOPIC = "rails-foo-topic";
    private final static String RAILS_BAR_TOPIC = "rails-bar-topic";

    // Accounts.
    private static final String ACCOUNT_XXX = "XXX-"+UUID.randomUUID();
    private static final String ACCOUNT_YYY = "YYY-"+UUID.randomUUID();
    private static final String ACCOUNT_ZZZ = "ZZZ-"+UUID.randomUUID();

    // One destination account for all tests.
    private static final String ACCOUNT_DEST = UUID.randomUUID().toString();

    private Consumer fooRailsConsumer;
    private Consumer barRailsConsumer;

    private static final Random RANDOM = new Random();

    @BeforeEach
    public void setup() {
        RestAssured.baseURI = ServiceClient.getInstance().getBaseUrl();

        fooRailsConsumer = KafkaClient.getInstance().createConsumer(GROUP_ID, RAILS_FOO_TOPIC);
        barRailsConsumer = KafkaClient.getInstance().createConsumer(GROUP_ID, RAILS_BAR_TOPIC);

        // Clear the topics.
        fooRailsConsumer.poll(Duration.ofSeconds(1));
        barRailsConsumer.poll(Duration.ofSeconds(1));
    }

    @AfterEach
    public void tearDown() {
        fooRailsConsumer.close();
        barRailsConsumer.close();
    }

    /**
     * Send in a single payment that is routed through to the FOO Rails topic.
     *
     * Verify the balance state is correct via the endpoint.
     */
    @Test
    public void testPaymentStreamsProcessing_SinglePayment_FooRails_NonFx() throws Exception {
        PaymentEvent payment = buildPaymentEvent(UUID.randomUUID().toString(),
                100L,
                "GBP",
                ACCOUNT_XXX,
                ACCOUNT_DEST,
                BANK_RAILS_FOO.name());
        KafkaClient.getInstance().sendMessage(PAYMENT_TOPIC, payment.getPaymentId(), JsonMapper.writeToJson(payment));
        List<ConsumerRecord<String, String>> outboundEvents = KafkaClient.getInstance().consumeAndAssert("SinglePayment_FooRails_NonFx", fooRailsConsumer, 1, 3);
        PaymentEvent result = JsonMapper.readFromJson(outboundEvents.get(0).value(), PaymentEvent.class);
        assertThat(result.getPaymentId(), equalTo(payment.getPaymentId()));
        assertThat(result.getRails(), equalTo(BANK_RAILS_FOO.name()));
        assertThat(result.getAmount(), equalTo(100L));
        assertThat(result.getCurrency(), equalTo("GBP"));

        get("/v1/kafka-streams/balance/"+ACCOUNT_XXX).then().assertThat()
                .statusCode(200)
                .and()
                .body(equalTo("100"));
    }

    /**
     * Send in a single payment that requires an FX rate transform, and is routed through to the BAR Rails topic.
     *
     * Verify the balance state is correct via the endpoint.
     */
    @Test
    public void testPaymentStreamsProcessing_SinglePayment_BarRails_Fx() throws Exception {
        PaymentEvent payment = buildPaymentEvent(UUID.randomUUID().toString(),
                100L,
                "USD",
                ACCOUNT_YYY,
                ACCOUNT_DEST,
                BANK_RAILS_BAR.name());
        KafkaClient.getInstance().sendMessage(PAYMENT_TOPIC, payment.getPaymentId(), JsonMapper.writeToJson(payment));
        List<ConsumerRecord<String, String>> outboundEvents = KafkaClient.getInstance().consumeAndAssert("SinglePayment_BarRails_Fx", barRailsConsumer, 1, 3);
        PaymentEvent result = JsonMapper.readFromJson(outboundEvents.get(0).value(), PaymentEvent.class);
        assertThat(result.getPaymentId(), equalTo(payment.getPaymentId()));
        assertThat(result.getRails(), equalTo(BANK_RAILS_BAR.name()));
        // FX hardcoded as 0.8.
        assertThat(result.getAmount(), equalTo(80L));
        // Currency converted to GBP.
        assertThat(result.getCurrency(), equalTo("GBP"));

        get("/v1/kafka-streams/balance/"+ACCOUNT_YYY).then().assertThat()
                .statusCode(200)
                .and()
                .body(equalTo("80"));
    }

    /**
     * Send in many payments with a mixture of amounts, currencies and rails.
     */
    @Test
    public void testPaymentStreamsProcessing_MultiplePayments() throws Exception {

        // The number of payments to send.
        int totalPayments = 1000;

        // Amount range to use.
        Long minAmount = 10L;
        Long maxAmount = 10000L;

        // Currencies to choose from.
        List<String> currencies = Arrays.asList(GBP.name(), USD.name());

        // Rails to choose from.
        List<String> rails = Arrays.asList(BANK_RAILS_FOO.name(), BANK_RAILS_BAR.name());

        // Track where payments are sent to.
        AtomicInteger fooRailsCount = new AtomicInteger(0);
        AtomicInteger barRailsCount = new AtomicInteger(0);
        AtomicLong balance = new AtomicLong(0);

        IntStream.range(0, totalPayments).parallel().forEach(i -> {
            PaymentEvent payment = buildPaymentEvent(UUID.randomUUID().toString(),
                    minAmount + (long) (Math.random() * (maxAmount - minAmount)),
                    currencies.get(RANDOM.nextInt(2)),
                    ACCOUNT_ZZZ,
                    ACCOUNT_DEST,
                    rails.get(RANDOM.nextInt(2)));
            try {
                KafkaClient.getInstance().sendMessage(PAYMENT_TOPIC, payment.getPaymentId(), JsonMapper.writeToJson(payment));

                // Track payments and totals from the randomised data.
                if(payment.getRails().equals(BANK_RAILS_FOO.name())) {
                    fooRailsCount.incrementAndGet();
                } else{
                    barRailsCount.incrementAndGet();
                }
                if(payment.getCurrency().equals(USD.name())) {
                    // Calculate the GBP amount.
                    balance.addAndGet(Math.round(payment.getAmount() * 0.8));
                } else{
                    balance.addAndGet(payment.getAmount());
                }
            } catch (Exception e) {
                fail(e);
            }
        });

        log.info("Expecting "+fooRailsCount.get()+" FOO Rails payments and "+barRailsCount.get()+" BAR Rails payments.");
        List<ConsumerRecord<String, String>> fooOutboundEvents = KafkaClient.getInstance().consumeAndAssert("MultiplePayments", fooRailsConsumer, fooRailsCount.get(), 3);
        List<ConsumerRecord<String, String>> barOutboundEvents = KafkaClient.getInstance().consumeAndAssert("MultiplePayments", barRailsConsumer, barRailsCount.get(), 3);
        assertThat((fooOutboundEvents.size() + barOutboundEvents.size()), equalTo(totalPayments));

        get("/v1/kafka-streams/balance/"+ACCOUNT_ZZZ).then().assertThat()
                .statusCode(200)
                .and()
                .body(equalTo(String.valueOf(balance.get())));
    }
}
