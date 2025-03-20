package com.sangyunpark.springkafka.consumer;

import com.sangyunpark.springkafka.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DeadLetterConsumer {
    // 메시지 처리 중 여러 번 재시도에도 실패한 메시지들이 저장되는 Dead Letter Queue(DLQ)

    @KafkaListener(topics = "orders.DLT", groupId = "dlt-group")
    public void listenDLT(@Payload OrderEvent order, Exception exception) {
        log.error("Received failed order in DLT: {}, Error: {}",
                    order.getOrderId(), exception.getMessage()
                );
    }
}
