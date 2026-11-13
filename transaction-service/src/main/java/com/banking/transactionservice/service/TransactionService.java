package com.banking.transactionservice.service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.entity.TransactionType;
import com.banking.transactionservice.event.TransactionCompletedEvent;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repository.TransactionRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {
	private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
	private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
	private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
	private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";

	private final TransactionRepository transactionRepository;
	private final AccountServiceClient accountServiceClient;
	private final KafkaTemplate<String, Object> kafkaTemplate;
	private final RedisTemplate<String, String> redisTemplate;

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

	/* find transaction history by sender account order by created at desc */
	public List<TransactionResponse> getTransactionHistory(String accountNumber) {
		return transactionRepository
				.findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber)
				.stream()
				.map(this::mapToResponse)
				.toList();
	}

	public TransactionResponse verifyOTP(String transactionId, String otp) {
		log.info("OTP verification for the transaction id: {}", transactionId);

		Transaction transaction =
				transactionRepository
						.findById(transactionId)
						.orElseThrow(() -> new RuntimeException("Transaction not found " + transactionId));

		String otpKey = "verification.otp" + transactionId;
		String storedOTP = redisTemplate.opsForValue().get(otpKey);

		if (storedOTP == null) {
			// Otp already expired
			log.warn("OTP expired for transaction: {}", transactionId);
			compensateTransaction(transaction, "OTP expired. Transaction cancelled and amount refunded");
			return mapToResponse(transaction);
		}

		if (!storedOTP.equals(otp)) {
			// BLOCK ACCOUNT AND REFUND
			log.warn("Wrong OTP - blocking account and refunding: {}", transactionId);
			redisTemplate.delete(otpKey);
			blockAccountAndCompensate(
					transaction, "Wrong OTP entered - transaction cancelled. Account blocked for security");
			return mapToResponse(transaction);
		}

		// OTP correct - complete the transaction
		log.info("OTP verified - completing transaction: {}", transactionId);
		redisTemplate.delete(otpKey);
		completeTransaction(transaction);
		return mapToResponse(transaction);
	}

	private void blockAccountAndCompensate(Transaction transaction, String reason) {
		// Publish fraud.detected -> Account service will block account
		Map<String, Object> fraudEvent = new HashMap<>();
		fraudEvent.put("transactionId", transaction.getId());
		fraudEvent.put("accountNumber", transaction.getSenderAccountNumber());
		fraudEvent.put("reason", reason);

		kafkaTemplate.send(FRAUD_DETECTED_TOPIC, transaction.getSenderAccountNumber(), fraudEvent);
		log.warn("Fraud detected published - account {} will be blocked. Kindly contact your bank.", transaction.getSenderAccountNumber());

		// SAGA COMPENSATION - refund sender
		compensateTransaction(transaction, reason);
	}

	private void completeTransaction(Transaction transaction) {
		transaction.setStatus(TransactionStatus.COMPLETED);
		transaction.setCompletedAt(LocalDateTime.now());
		transactionRepository.save(transaction);

		TransactionCompletedEvent completedEvent = new TransactionCompletedEvent(
				transaction.getId(),
				transaction.getSenderAccountNumber(),
				transaction.getReceiverAccountNumber(),
				transaction.getAmount(),
				transaction.getDescription()
		);

		kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC, transaction.getId(), completedEvent);

		log.info("SAGA completed. Transaction {} completed", transaction.getId());
	}

	private void compensateTransaction(Transaction transaction, String reason) {
		log.warn(
				"SAGA compensation - refunding {} amount {}",
				transaction.getSenderAccountNumber(),
				transaction.getAmount());
		accountServiceClient.creditBalance(
				transaction.getSenderAccountNumber(), transaction.getAmount());
		transaction.setStatus(TransactionStatus.FLAGGED);
		transaction.setFailureReason(
				reason + " SAGA Compensation executed, amount refunded at " + LocalDateTime.now());
		transactionRepository.save(transaction);

		// publish refund event - Notification service will alert user refund has been received
		Map<String, Object> refundEvent = new HashMap<>();
		refundEvent.put("transactionId", transaction.getId());
		refundEvent.put("senderAccountNumber", transaction.getSenderAccountNumber());
		refundEvent.put("amount", transaction.getAmount());
		refundEvent.put("reason", reason);

		kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC, transaction.getId(), refundEvent);

		log.info("SAGA Compensation COMPLETE - {}. refunded to {}", transaction.getAmount(),
				transaction.getSenderAccountNumber());
	}

	public void processCleanResult(String transactionId) {
		Transaction transaction = transactionRepository
				.findById(transactionId)
				.orElseThrow(() -> new RuntimeException("Transaction not found " + transactionId));
		if (transaction.getStatus() != TransactionStatus.PROCESSING) {
			log.warn("Transaction {} not PROCESSING - skipping", transactionId);
			return;
		}

		completeTransaction(transaction);
	}
}
