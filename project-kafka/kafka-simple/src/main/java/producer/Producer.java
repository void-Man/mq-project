package producer;

import cn.hutool.core.lang.Snowflake;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import vo.OrderVo;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author mengjie_chen
 * @description
 * @date 2021/9/20
 */
@Slf4j
public class Producer {
    private static final String TOPIC_NAME = "kafka-03";
    private static final Callback CALL_BACK = (metadata, e) -> System.out.println("异步方式发送消息结果：" + "topic-" + metadata.topic() + "|partition-" + metadata.partition() + "|offset-" + metadata.offset());

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = getProp();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        int msgNum = 10;

        for (int i = 0; i < msgNum; i++) {
            OrderVo orderVo = OrderVo.builder().orderId(new Snowflake().nextId()).amount(new BigDecimal(i * 100)).build();
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, 1, String.valueOf(orderVo.getOrderId()), JSONObject.toJSONString(orderVo));
            // 异步发送消息
            producer.send(record, CALL_BACK);

            /*// 同步发送消息
            RecordMetadata metadata = producer.send(record).get();
            System.out.println("同步方式发送消息结果：" + "topic-" + metadata.topic() + "|partition-" + metadata.partition() + "|offset-" + metadata.offset());*/
        }

        producer.close();

    }

    private static Properties getProp() {
        Properties properties = new Properties();
        // Kafka 服务器地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.198.153:9092,192.168.198.154:9092,192.168.198.155:9092");

        // 发出消息持久化机制参数
        //（1）acks=0： 表示producer不需要等待任何broker确认收到消息的回复，就可以继续发送下一条消息。性能最高，但是最容易丢消息。
        //（2）acks=1： 至少要等待leader已经成功将数据写入本地log，但是不需要等待所有follower是否成功写入。就可以继续发送下一条消息。这种情况下，如果follower没有成功备份数据，而此时leader又挂掉，则消息会丢失。
        //（3）acks=-1或all： 需要等待 min.insync.replicas(默认为1，推荐配置大于等于2) 这个参数配置的副本个数都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据。这是最强的数据保证。一般除非是金融级别，或跟钱打交道的场景才会使用这种配置。
        properties.put(ProducerConfig.ACKS_CONFIG, "1");

        // 重试次数，发送失败会重试，默认重试间隔100ms，重试能保证消息发送的可靠性，但是也可能造成消息重复发送，比如网络抖动，所以需要在接收者那边做好消息接收的幂等性处理
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        // 重试间隔设置
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 300);

        // 本地缓冲区大小。设置发送消息的本地缓冲区，如果设置了该缓冲区，消息会先发送到本地缓冲区，可以提高消息发送性能，默认值是33554432，即32MB
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        // 生产端批量发送大小。kafka本地线程会从缓冲区取数据，批量发送到broker，设置批量发送消息的大小，默认值是16384，即16kb，就是说一个batch满了16kb就发送出去
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        // 间隔多久必须发送一次。默认值是0，意思就是消息必须立即被发送，但这样会影响性能，一般设置10毫秒左右。
        // 就是说这个消息发送完后会进入本地的一个batch，如果10毫秒内，这个batch满了16kb就会随batch一起被发送出去
        // 如果10毫秒内，batch没满，那么也必须把消息发送出去，不能让消息的发送延迟时间太长
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 10);

        //把发送的key从字符串序列化为字节数组
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //把发送消息value从字符串序列化为字节数组
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

}
