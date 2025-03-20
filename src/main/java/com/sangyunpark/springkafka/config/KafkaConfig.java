package com.sangyunpark.springkafka.config;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import com.sangyunpark.springkafka.model.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Bean
    public ProducerFactory<String, OrderEvent> producerFactory() { // Producer 생성
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // 브로커 주소
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class); // 메시지키 직렬화
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class); // 메시지 값 직렬화 OrderEvent 객체 -> JSON 형식 직렬화
        return new DefaultKafkaProducerFactory<>(props); // 인스턴스 생성
    }

    @Bean
    public KafkaTemplate<String, OrderEvent> kafkaTemplate() { // 메시지 전송시 사용하는 템플릿 객체
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConsumerFactory<String, OrderEvent> consumerFactory() { // Kafka 컨슈머를 생성하기 위한 설정을 담당
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // 브로커 주소 설정
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-group"); // 컨슈머 그룹 ID
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class); // 메시지 키 역직렬화
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class); // 메시지 값 역직렬화
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.sangyunpark.spring-kafka.model"); // 역직렬화시 신뢰하는 패키지 지정
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean // 리스너 컨테이너를 생성해 여러 스레드에서 메시지를 처리할 수 있게 해줍니다.
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        return factory;
    }
}
