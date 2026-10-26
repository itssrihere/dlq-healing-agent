package com.example.dlqagent.orders;

import java.math.BigDecimal;
import java.time.Instant;

/** The order event as the downstream consumer expects it. {@code orderId} is also the Kafka key. */
public record OrderEvent(
        String orderId,
        String customerId,
        BigDecimal amount,
        String currency,
        OrderStatus status,
        Instant createdAt) {}
