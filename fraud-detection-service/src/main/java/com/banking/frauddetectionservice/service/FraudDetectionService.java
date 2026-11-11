package com.banking.frauddetectionservice.service;

import com.banking.frauddetectionservice.client.AccountServiceClient;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
public class FraudDetectionService {
  private AccountServiceClient accountServiceClient;

  final void checkTransaction(Map<String, Object> payload) {
    String transactionId = (String) payload.get("transactionId");
    String accountNumber = (String) payload.get("senderAccountNumber");
    BigDecimal amount = new BigDecimal(payload.get("amount").toString());

    // fetch the real balance from Account Service

  }
}
