package com.banking.transactionservice.service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.entity.TransactionType;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repository.TransactionRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {
  private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
  private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
  private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";

  private final TransactionRepository transactionRepository;
  private final AccountServiceClient accountServiceClient;
  private final KafkaTemplate<String, Object> kafkaTemplate;

  public TransactionResponse getTransaction(String transactionId) {
    return mapToResponse(
        transactionRepository
            .findById(transactionId)
            .orElseThrow(() -> new RuntimeException("Transaction not found: " + transactionId)));
  }

  /*
   * Saga Step 1: Initiate Transfer
   * Deducts from sender via feign client
   * Saves transaction as processing
   * Publish event to kafka for fraud check
   * Returns
   * @param request
   * @returns TransactionResponse
   */
  public TransactionResponse transfer(@Valid TransferRequest request) {

    log.info(
        "SAGA START: Transfer {} -> {} amount {}",
        request.getSenderAccountNumber(),
        request.getReceiverAccountNumber(),
        request.getAmount());

    // Sage step1: Deduct from sender
    accountServiceClient.deductBalance(request.getSenderAccountNumber(), request.getAmount());

    Transaction transaction = new Transaction();
    transaction.setSenderAccountNumber(request.getSenderAccountNumber());
    transaction.setReceiverAccountNumber(request.getReceiverAccountNumber());
    transaction.setAmount(request.getAmount());
    transaction.setType(TransactionType.TRANSFER);
    transaction.setStatus(TransactionStatus.PROCESSING);
    transaction.setDescription(request.getDescription());
    transaction.setReferenceNumber(UUID.randomUUID().toString());

    Transaction savedTransaction = transactionRepository.save(transaction);
    log.info("Transaction saved as Processing: {}", savedTransaction.getId());

    // Saga Step2: Publish for fraud check
    TransactionInitiatedEvent event =
        new TransactionInitiatedEvent(
            savedTransaction.getId(),
            savedTransaction.getSenderAccountNumber(),
            savedTransaction.getReceiverAccountNumber(),
            savedTransaction.getAmount(),
            savedTransaction.getDescription());
    kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC, savedTransaction.getId(), event);
    log.info("Saga step 2 - Transaction Initiated Event Published: {}", savedTransaction.getId());
    return mapToResponse(savedTransaction);
  }

  private TransactionResponse mapToResponse(Transaction transaction) {

    TransactionResponse response = new TransactionResponse();
    response.setId(transaction.getId());
    response.setSenderAccountNumber(transaction.getSenderAccountNumber());
    response.setReceiverAccountNumber(transaction.getReceiverAccountNumber());
    response.setAmount(transaction.getAmount());
    response.setType(transaction.getType());
    response.setStatus(transaction.getStatus());
    response.setDescription(transaction.getDescription());
    response.setReferenceNumber(transaction.getReferenceNumber());
    response.setFailureReason(transaction.getFailureReason());
    response.setCreatedAt(transaction.getCreatedAt());
    response.setCompletedAt(transaction.getCompletedAt());
    return response;
  }

  public List<TransactionResponse> getTransactionHistory(String accountNumber) {
    return transactionRepository
        .findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber)
        .stream()
        .map(this::mapToResponse)
        .toList();
  }

  public Object verifyOTP(String transactionId, String otp) {

    return null;
  }
}
