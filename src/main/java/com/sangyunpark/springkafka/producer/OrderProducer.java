package com.sangyunpark.springkafka.producer;

import com.sangyunpark.springkafka.model.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class OrderProducer { // 프로듀서 구현

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate; // OrderEvent 객체 전송
    private static final String TOPIC = "orders"; // 메시지를 전송할 Kafka 토픽의 이름

    public void sendOrder(OrderEvent order) { // 비동기 메시지 전송
        // 비동기로 동작하기 때문에, 즉시 Future 객체(여기서는 CompletableFuture 또는 ListenableFuture와 유사)를 반환합니다.
        // 애플리케이션의 다른 작업과 변행해서 메시지 전송을 수행할 수 있습니다.
        kafkaTemplate.send(TOPIC, order.getOrderId(), order)
                .whenComplete((result, ex) -> {
                    if(ex != null) { // 예외가 발생하는 경우
                        log.error("Failed to send message: {}", order.getOrderId(), ex);
                    } else { // 예외가 발생하지 않는 경우
                        log.info("Message send successfully: {}, partition: {}", order.getOrderId(), result.getRecordMetadata().partition());
                    }
                });
    }

    public void sendOrderSync(OrderEvent order) throws Exception { // 동기 메시지 전송
        try {
            // get() 메서드를 호출해서 Future 결과를 블로킹 방식으로 기다린다.
            // 전송 결과를 즉시 확인해야 하는 경우 사용합니다.
            SendResult<String, OrderEvent> result = kafkaTemplate.send(TOPIC, order.getOrderId(), order).get();
            log.info("Message sent synchronously: {}, partition: {}", order.getOrderId(), result.getRecordMetadata().partition());
        } catch (Exception e) {
            log.error("Error sending message synchronously", e);
            throw e;
        }
    }
}
