package com.cmj.example.vo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author mengjie_chen
 * @description
 * @date 2021/9/20
 */
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class OrderVo {
    private long orderId;
    private BigDecimal amount;
}
