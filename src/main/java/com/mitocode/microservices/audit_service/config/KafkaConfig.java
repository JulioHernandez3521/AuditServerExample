package com.mitocode.microservices.audit_service.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mitocode.microservices.audit_service.repository.ProductRepository;
import com.mitocode.microservices.common_models.model.entty.AuditInfo;
import com.mitocode.microservices.common_models.model.entty.GenericEntity;
import com.mitocode.microservices.common_models.model.entty.ProductEntity;
import com.mitocode.microservices.common_models.model.entty.UserEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

    private final ObjectMapper mapper;
    private final ProductRepository repository;
    private final ProductRepository productRepository;

    @Value("${kafka.mitocode.server:127.0.0.1}")
    private String kafkaServer;
    @Value("${kafka.mitocode.port:9092}")
    private String kafkaPort;
    @Value("${kafka.mitocode.topicName:mitocode}")
    private String topicName;

    @Bean
    public ConsumerFactory<String, GenericEntity<?>> consumerFactory() {
        Map<String, Object> kafkaProps = new HashMap<>();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer + ":" + kafkaPort);
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, topicName);

//        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        kafkaProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, JsonDeserializer.class);
        kafkaProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());

        kafkaProps.put(JsonDeserializer.KEY_DEFAULT_TYPE, "com.mitocode.microservices.audit_service.config.KafkaConfig");
        kafkaProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.mitocode.microservices.audit_service.config.KafkaConfig");

        kafkaProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.mitocode.microservices.*");

        return new DefaultKafkaConsumerFactory<>(kafkaProps);
    }


    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, GenericEntity<?>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, GenericEntity<?>> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }


    @KafkaListener(topics = "mitocode")
    public void listenTopic(GenericEntity<?> message) {

//        log.info("=====>" + message.getT());

        if (message.getClass().getSimpleName().equals(AuditInfo.class.getSimpleName())) {

            AuditInfo auditInfo = mapper.convertValue(message.getT(), new TypeReference<>() {
            });
            log.info("Entidad Audit Info: " + auditInfo);

        } else if (message.getClass().getSimpleName().equals(ProductEntity.class.getSimpleName())) {

            ProductEntity productEntity = mapper.convertValue(message.getT(), new TypeReference<>() {
            });
            log.info("Entidad Product Entity: " + productEntity);
            productRepository.save(productEntity);

        } else if (message.getClass().getSimpleName().equals(UserEntity.class.getSimpleName())) {

            UserEntity userEntity = mapper.convertValue(message.getT(), new TypeReference<>() {
            });
            log.info("Entidad User Entity: " + userEntity);

        } else {
            log.info("..::.." + message.getClass().getSimpleName());
            log.info("=====>" + message.getClass());
        }
    }


}
