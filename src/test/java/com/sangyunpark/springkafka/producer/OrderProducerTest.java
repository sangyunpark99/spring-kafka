package com.sangyunpark.springkafka.producer;

import com.sangyunpark.springkafka.model.OrderEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@SpringBootTest
public class OrderProducerTest {

    @Autowired
    private OrderProducer producer;

    @Autowired
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    private Consumer<String, OrderEvent> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "localhost:9092",
                "test-group",
                "true"
        );

        consumer = new DefaultKafkaConsumerFactory<>(
                consumerProps,
                new StringDeserializer(),
                new JsonDeserializer<>(OrderEvent.class)
        ).createConsumer();

        consumer.subscribe(List.of("orders"));
        consumer.poll(Duration.ofMillis(100));
        consumer.seekToBeginning(consumer.assignment());
    }
}
