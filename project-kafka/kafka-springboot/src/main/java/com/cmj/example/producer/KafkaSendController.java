package com.cmj.example.producer;

import cn.hutool.core.lang.Snowflake;
import com.alibaba.fastjson.JSONObject;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.cmj.example.vo.OrderVo;

import javax.annotation.Resource;
import java.math.BigDecimal;

/**
 * @author mengjie_chen
 * @description
 * @date 2021/9/20
 */
@RestController
@RequestMapping("kafka")
public class KafkaSendController {

    private final static String TOPIC_NAME = "spring-boot-topic";
    private final static Snowflake SNOW_FLAKE = new Snowflake();

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("send")
    public void send(int partition) {
        OrderVo orderVo = OrderVo.builder().orderId(SNOW_FLAKE.nextId()).amount(new BigDecimal(SNOW_FLAKE.nextId())).build();
        kafkaTemplate.send(TOPIC_NAME, partition, String.valueOf(orderVo.getOrderId()), JSONObject.toJSONString(orderVo));
    }

}
