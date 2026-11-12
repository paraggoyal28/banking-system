package com.banking.transactionservice.service;

import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionEventConsumer {
  private static final String VERIFICATION_REQUIRED_TOPIC = "verification.required";
  private static final String TRANSACTION_OTP_GENERATED_TOPIC = "transaction.otp.generated";

  private static final String TRANSACTION_GROUP = "transaction-service-group";
  private static final long OTP_EXPIRY_MINUTES = 5;
  private final TransactionRepository transactionRepository;
  private final RedisTemplate<String, String> redisTemplate;
  private final KafkaTemplate<String, Object> kafkaTemplate;

  /*
   * consume verification required
   * Generate OTP and ask user to verify
   * @param payload
   */
  @KafkaListener(topics = VERIFICATION_REQUIRED_TOPIC, groupId = TRANSACTION_GROUP)
  public void consumeVerificationRequired(@Payload Map<String, Object> payload) {

    try {
      String transactionId = (String) payload.get("transactionId");
      String accountNumber = (String) payload.get("accountNumber");
      String reason = (String) payload.get("reason");

      log.info("Verification Required - transaction: {}, reason: {}", transactionId, reason);
      Transaction transaction =
          transactionRepository
              .findById(transactionId)
              .orElseThrow(() -> new RuntimeException("Transaction not found: " + transactionId));

      if (transaction.getStatus() != TransactionStatus.PROCESSING) {
        log.warn("Transaction is not processing - skipping ", transactionId);
        return;
      }

      // Generate 6 digit OTP
      String otp = String.format("%06d", (int) (Math.random() * 900000) + 100000);

      // String OTP expires in five minutes
      String otpKey = "verification.otp" + transactionId;
      redisTemplate.opsForValue().set(otpKey, otp, OTP_EXPIRY_MINUTES, TimeUnit.MINUTES);

      // Update status
      transaction.setStatus(TransactionStatus.PENDING_VERIFICATION);
      transactionRepository.save(transaction);

      log.info(
          "Otp generated for transaction: {}, expires in {} min",
          transactionId,
          OTP_EXPIRY_MINUTES);

      // NOTIFY USER
      Map<String, Object> otpEvent = new HashMap<>();
      otpEvent.put("transactionId", transactionId);
      otpEvent.put("accountNumber", accountNumber);
      otpEvent.put("reason", reason);
      otpEvent.put("otp", otp);
      otpEvent.put("amount", payload.get("amount"));

      kafkaTemplate.send(TRANSACTION_OTP_GENERATED_TOPIC, transactionId, otpEvent);
    } catch (Exception e) {
      log.error("Error handling verification required: {}", e.getMessage(), e);
    }
  }
}
