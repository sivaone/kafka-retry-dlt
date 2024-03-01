package com.github.sivaone.kafka.config;

import com.github.sivaone.kafka.exception.RetryableException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaRetryTopic;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.retrytopic.*;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableKafka
//@EnableKafkaRetryTopic
//@EnableScheduling
public class KafkaConsumerConfig
//        extends RetryTopicConfigurationSupport
{

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";

//    @RetryableTopic
//    public void consumerConfig() {
//
//    }

//    @Override
//    public RetryTopicConfigurer retryTopicConfigurer(
//            KafkaConsumerBackoffManager kafkaConsumerBackoffManager,
//            DestinationTopicResolver destinationTopicResolver,
//            ObjectProvider<RetryTopicComponentFactory> componentFactoryProvider,
//            BeanFactory beanFactory
//    ) {
//
//
//        return super.retryTopicConfigurer(kafkaConsumerBackoffManager, destinationTopicResolver, componentFactoryProvider, beanFactory);
//    }

    @Bean
    public RetryTopicConfiguration myRetryTopic(
            @Qualifier("orderKafkaTemplate") KafkaTemplate<String, String> template) {
//        TopicSuffixingStrategy topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_DELAY_VALUE;
//        SameIntervalTopicReuseStrategy sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC;
        return RetryTopicConfigurationBuilder
                .newInstance()
                .autoCreateTopics(false, 1, Short.valueOf("1"))
                .includeTopic("order")
                .retryTopicSuffix("-retry")
                .dltSuffix("-dlt")
                .useSingleTopicForSameIntervals()
                //.setTopicSuffixingStrategy(topicSuffixingStrategy)
                //.sameIntervalTopicReuseStrategy(sameIntervalTopicReuseStrategy)
//                .dltHandlerMethod("orderConsumer", "dltHandler") // to process dlt messages
                .autoStartDltHandler(false) // Set false to not to process the dlt messages
                .fixedBackOff(4000)
                .maxAttempts(5)
                .concurrency(1)
                .timeoutAfter(60000)
                .retryOn(RetryableException.class)
//                .notRetryOn(Collections.emptyList())
//                .listenerFactory("kafkaListenerContainerFactory") // can have different listener for retry topic
                .create(template);
    }

    @Bean("orderKafkaTemplate")
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(kafkaProducerFactory());
    }

    @Bean
    public ProducerFactory<String, String> kafkaProducerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfiguration());
//        return new DefaultKafkaProducerFactory<>(
//                producerConfiguration(),
//                new StringSerializer(),
//                new StringSerializer()
//        );
    }


    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> orderCluster() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConcurrency(1);
//        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setConsumerFactory(consumerFactory());

        return factory;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfiguration());
    }

    private Map<String, Object> producerConfiguration() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //config.put(ProducerConfig.RETRIES_CONFIG, 3); // resend a rec if send fails with a transient error (high imp)
//        config.put(ProducerConfig.CLIENT_ID_CONFIG, "orders"); // Just to track who sent the message
//        config.put(ProducerConfig.ACKS_CONFIG, "all"); // 0, 1, all (low imp)
        return config;
    }

    private Map<String, Object> consumerConfiguration() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "my-orders"); // (high imp)
//        config.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-orders-client"); // Just to track who consumed the message
//        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // [latest, earliest, none] (medium imp)
        return config;
    }

    // TODO: seems to be not working, need to fix
    @Bean
    DeadLetterPublishingRecovererFactory deadLetterPublishingRecovererFactory(DestinationTopicResolver resolver) {
        DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(resolver);
        factory.setDeadLetterPublishingRecovererCustomizer(dlpr -> {
            dlpr.setAppendOriginalHeaders(false);
            dlpr.setStripPreviousExceptionHeaders(true);
        });
        return factory;
    }

    // TODO: strip off stack trace from retry topic headers

}
