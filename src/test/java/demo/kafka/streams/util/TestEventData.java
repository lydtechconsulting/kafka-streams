package demo.kafka.streams.util;

import demo.kafka.streams.event.PaymentEvent;

public class TestEventData {

    public static PaymentEvent buildPaymentEvent(String id, Long amount, String currency, String fromAccount, String toAccount, String rails) {
        return PaymentEvent.builder()
                .paymentId(id)
                .amount(amount)
                .currency(currency)
                .fromAccount(fromAccount)
                .toAccount(toAccount)
                .rails(rails)
                .build();
    }
}
