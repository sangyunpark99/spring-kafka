package com.sangyunpark.springkafka.consumer;

import com.sangyunpark.springkafka.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class OrderConsumer {

    // orders 토픽에서 메시지를 수신하도록 지정
    // groupId를 통해 같은 그룹 내의 다른 컨슈머들과 메시지를 분산 처리
    @KafkaListener(topics = "orders", groupId = "order-group")
    public void listen(
            @Payload OrderEvent order, // 수신된 메시지 OrderEvent 객체
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition, // 파티션 정보
            @Header(KafkaHeaders.OFFSET) long offset // 오프셋 전달
    ) {
//        try {
            log.info("Received order: {}, partition: {}, offset: {}", order.getOrderId(), partition, offset);
            processOrder(order);
//        }catch (Exception e) {
//            log.error("Error processing order: {}", order.getOrderId(), e);
//            handleError(order,e);
//        }
    }

    protected void processOrder(OrderEvent order) {
        // 주문 처리 로직
        log.info("Processing order:{}", order.getOrderId());
        // 추가 비즈니스 로직 실행
    }

    private void handleError(OrderEvent order, Exception e) {
        // 에러 처리 로직
    }
}
