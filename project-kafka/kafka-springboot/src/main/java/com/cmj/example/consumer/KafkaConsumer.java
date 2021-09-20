package com.cmj.example.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * @author mengjie_chen
 * @description
 * @date 2021/9/20
 */
@Component
@Slf4j
public class KafkaConsumer {
    private final static String TOPIC_NAME = "spring-boot-topic";

    /**
     * groupId：消费组名称
     * topicPartitions：消费主题和分区信息
     *
     * @param record
     * @param ack
     * @return void
     * @date 2021/9/20
     */
    @KafkaListener(groupId = "spring_boot_group", topicPartitions = {@TopicPartition(topic = TOPIC_NAME, partitions = {"0"})})
    public void consumer00(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String value = record.value();
        log.info("group {} consumer00 accept msg -----> {}", "spring_boot_group", value);
        //手动提交offset
        ack.acknowledge();
    }

    /**
     * groupId：消费组名称
     * topicPartitions：消费主题和分区信息
     *
     * @param record
     * @param ack
     * @return void
     * @date 2021/9/20
     */
    @KafkaListener(groupId = "spring_boot_group", topicPartitions = {@TopicPartition(topic = TOPIC_NAME, partitions = {"1"})})
    public void consumer01(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String value = record.value();
        log.info("group {} consumer01 accept msg -----> {}", "spring_boot_group", value);
        //手动提交offset
        ack.acknowledge();
    }

    /**
     * groupId：消费组名称
     * topicPartitions：消费主题和分区信息
     *
     * @param record
     * @param ack
     * @return void
     * @date 2021/9/20
     */
    @KafkaListener(groupId = "spring_boot_group", topicPartitions = {@TopicPartition(topic = TOPIC_NAME, partitions = {"2"})})
    public void consumer02(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String value = record.value();
        log.info("group {} consumer02 accept msg -----> {}", "spring_boot_group", value);
        //手动提交offset
        ack.acknowledge();
    }

}
