package com.github.sivaone.kafka.config;

import com.github.sivaone.kafka.exception.RetryableException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

//    @RetryableTopic(kafkaTemplate = "kafkaTemplate")
    @KafkaListener(topics = {"order"}, containerFactory = "orderCluster", groupId = "my-orders")
    public void listen(ConsumerRecord<String, String> record) {
        log.atInfo().log("Consumed from topic {}", record.topic());
        log.atInfo().log(record.toString());

//        String recordStr = "ConsumerRecord(topic = " + record.topic() + ", partition = " + record.partition() + ", offset = "
//                + record.offset() + ", " + record.timestampType() + " = " + record.timestamp() + ", key = " + record.key()
//                + ", value = " + record.value() + ")";
        if (record.value().equalsIgnoreCase("test-error-msg")) {
            throw new RetryableException("error in consumer");
        }
    }

//    @DltHandler
//    public void dltHandler(ConsumerRecord<String, String> record) {
//        log.atInfo().log("Consumed from topic {}", record.topic());
//        log.atInfo().log(record.toString());
//    }
}
