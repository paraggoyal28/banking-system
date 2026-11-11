package com.banking.accountservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountEventConsumer {
  private final AccountService accountService;

  /*
   * Consume transaction completed event from kafka
   * no fraud detected, credits receiver account
   * @param payload
   */
  @KafkaListener(topics = "transaction.completed")
  public void consumeTransactionCompleted(@Payload Map<String, Object> payload) {
    try {
      String receiverAccount = (String) payload.get("receiverAccountNumber");
      BigDecimal amount = new BigDecimal(payload.get("amount").toString());

      log.info("Crediting amount: {} to account: {}", amount, receiverAccount);
      accountService.creditBalance(receiverAccount, amount);

    } catch (Exception e) {
      log.error("Error crediting account: {}", e.getMessage(), e);
    }
  }

  /*
   * consume fraud detected event from kafka
   * blocks the flagged account
   * @param payload
   */
  @KafkaListener(topics = "fraud.detected")
  public void consumeFraudDetected(@Payload Map<String, Object> payload) {
    try {

      String accountNumber = (String) payload.get("accountNumber");
      log.info("Fraud detected. Blocking the account: {}", accountNumber);

      accountService.block(accountNumber);

    } catch (Exception e) {
      log.error("Error blocking account: {}", e.getMessage(), e);
    }
  }
}
