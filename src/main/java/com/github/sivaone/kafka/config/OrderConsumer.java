package com.github.sivaone.kafka.config;

import com.github.sivaone.kafka.exception.NonRetryableException;
import com.github.sivaone.kafka.exception.RetryableException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import static org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy.SINGLE_TOPIC;

@Component
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    // First approach: Use in conjunction with KafkaConsumerConfig.myRetryTopic() configuration
    //    @KafkaListener(topics = {"order"}, containerFactory = "orderCluster", groupId = "my-orders")
    public void listen(ConsumerRecord<String, String> record) {
        log.atInfo().log("Consumed from topic {}", record.topic());
        String message = record.value();
        log.atInfo().log("Payload : {}", message);

//        String recordStr = "ConsumerRecord(topic = " + record.topic() + ", partition = " + record.partition() + ", offset = "
//                + record.offset() + ", " + record.timestampType() + " = " + record.timestamp() + ", key = " + record.key()
//                + ", value = " + record.value() + ")";
        processMessage(message);
    }

    // Second approach: Use this @RetryableTopic or above method with KafkaConsumerConfig.myRetryTopic()
    @RetryableTopic(
            kafkaTemplate = "orderKafkaTemplate",
            attempts = "5",
            backoff = @Backoff(delay = 120000L),
            timeout = "60000",
            autoCreateTopics = "false",
            // numPartitions = "3",
            // replicationFactor = "1",
            include = {RetryableException.class},
            //exclude = {NonRetryableException.class}, // either include or exclude only
            retryTopicSuffix = "-retry",
            dltTopicSuffix = "-dlt",
            autoStartDltHandler = "false",
            sameIntervalTopicReuseStrategy = SINGLE_TOPIC,
            concurrency = "3"
    )
    @KafkaListener(topics = {"order"}, containerFactory = "orderCluster", groupId = "my-second-orders")
    public void listenAnnotation(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.atInfo().log("Topic : {}", topic);
        log.atInfo().log("Key : {}, Payload : {}", key, message);
        processMessage(message);
    }

    private static void processMessage(String message) {
        if (message.equalsIgnoreCase("retry-error-msg")) {
            throw new RetryableException("Retry error in consumer");
        } else if (message.equalsIgnoreCase("dlt-error-msg")) {
            throw new NonRetryableException("Dlt error in consumer");
        }
    }

//    @DltHandler
//    public void dltHandler(ConsumerRecord<String, String> record) {
//        log.atInfo().log("Consumed from topic {}", record.topic());
//        log.atInfo().log(record.toString());
//    }
}
