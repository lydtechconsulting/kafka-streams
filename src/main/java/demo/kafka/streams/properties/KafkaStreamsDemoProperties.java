package demo.kafka.streams.properties;

import javax.validation.constraints.NotNull;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

@Configuration
@ConfigurationProperties("kafkastreamsdemo")
@Getter
@Setter
@Validated
public class KafkaStreamsDemoProperties {
    @NotNull private String id;
    @NotNull private String paymentInboundTopic;
    @NotNull private String railsFooOutboundTopic;
    @NotNull private String railsBarOutboundTopic;
}
