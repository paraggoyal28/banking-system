package com.banking.frauddetectionservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionEventConsumer {
  private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";

  private final FraudDetectionService fraudDetectionService;

  /*
   * Listens to transaction.initiated topic
   * Every transaction goes through fraud check before completing
   * @param payload
   */
  @KafkaListener(topics = TRANSACTION_INITIATED_TOPIC, groupId = "fraud-detection-group")
  public void consumeTransactionInitiated(@Payload Map<String, Object> payload) {
    // check the payload from the transaction initiated event
    log.info("Received transaction for fraud check: {}", payload.get("transactionId"));

    try {
      fraudDetectionService.checkTransaction(payload);

    } catch (Exception e) {

    }
  }
}
