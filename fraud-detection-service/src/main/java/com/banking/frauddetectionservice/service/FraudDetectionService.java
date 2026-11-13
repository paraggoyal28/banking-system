package com.banking.frauddetectionservice.service;

import com.banking.frauddetectionservice.client.AccountServiceClient;
import com.banking.frauddetectionservice.model.FraudCheckResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionService {
	private static final String VERIFICATION_REQUIRED_TOPIC = "verification.required";
	private static final String FRAUD_CHECK_CLEAN_RESULT_TOPIC = "fraud.check.clean";
	private final AccountServiceClient accountServiceClient;
	private final KafkaTemplate<String, Object> kafkaTemplate;
	private final RedisTemplate<String, String> redisTemplate;

	@Value("${fraud.max-transactions-per-minute}")
	private int maxTransactionsPerMinute;

	@Value("${fraud.suspicious-amount-multiplier}")
	private double suspiciousAmountMultiplier;

	@Value("${fraud.max-balance-percentage}")
	private double maxBalancePercentage;

	public void checkTransaction(Map<String, Object> payload) {
		String transactionId = (String) payload.get("transactionId");
		String accountNumber = (String) payload.get("senderAccountNumber");
		BigDecimal amount = new BigDecimal(payload.get("amount").toString());

		// fetch the real balance from Account Service
		// 95% of balance is being initiated - so detect fraud
		BigDecimal senderBalance = accountServiceClient.getBalance(accountNumber);
		log.info(
				"Checking transaction: {}, account: {}, amount: {}, balance: {}",
				transactionId,
				accountNumber,
				amount,
				senderBalance);

		FraudCheckResult result = performFraudCheck(accountNumber, amount, senderBalance);

		// suspicious activity detected
		if (result.isFraud()) {
			log.info(
					"Suspicious activity detected in account; {}, reason: {}. Requesting OTP verification",
					accountNumber,
					result.getReason());

			Map<String, Object> verificationEvent = new HashMap<>();
			verificationEvent.put("transactionId", transactionId);
			verificationEvent.put("accountNumber", accountNumber);
			verificationEvent.put("amount", amount);
			verificationEvent.put("reason", result.getReason());

			kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC, transactionId, verificationEvent);
		} else {
			// transaction is clean
			log.info("Transaction clean");
			Map<String, Object> transactionCleanEvent = new HashMap<>();
			transactionCleanEvent.put("transactionId", transactionId);
			transactionCleanEvent.put("isFraud", false);
			transactionCleanEvent.put("reason", null);

			kafkaTemplate.send(FRAUD_CHECK_CLEAN_RESULT_TOPIC, transactionId, transactionCleanEvent);
		}
	}

	private FraudCheckResult performFraudCheck(
			String accountNumber, BigDecimal amount, BigDecimal senderBalance) {

    /*
         velocity check - fraudsters don't withdraw manually, run scripts to multiple transactions simultaneously
       5, 10, or 20 transactions per minute
       detect above as suspicious
    or 3x, 5x of average transaction a user usually makes
    get average transactions of a user 50, 50, 45, 100, 65, 35, 10, 120, 56
    average = 70
    If user starts transaction around 10000, 50000, 11000
    the account is suspicious marked, ask the user to verify
    balance check if transaction is 90% of users balance - suspicious
         */
		// Pattern1 - Velocity check
		if (isVelocityExceeded(accountNumber)) {
			return new FraudCheckResult(
					true, "Too many transactions in 60 seconds. Velocity limit exceeded");
		}

		// Pattern2 - Amount check
		if (isAmountSuspicious(accountNumber, amount)) {
			return new FraudCheckResult(
					true, "Unusual transaction amount " + " - exceeds 3 times your average");
		}

		// Pattern3: Balance check
		if (senderBalance.compareTo(BigDecimal.ZERO) > 0
				&& isBalanceCheckFailed(senderBalance, amount)) {
			return new FraudCheckResult(true, "Transaction exceeds 90% of account balance");
		}

		return new FraudCheckResult(false, null);
	}

	private boolean isBalanceCheckFailed(BigDecimal senderBalance, BigDecimal amount) {
		BigDecimal maximumAllowed = senderBalance.multiply(BigDecimal.valueOf(maxBalancePercentage));
		log.info(
				"Balance check - amount: {}, maxAllowed: {}, suspicious: {}",
				amount,
				maximumAllowed,
				amount.compareTo(maximumAllowed) > 0);
		return amount.compareTo(maximumAllowed) > 0;
	}

	private boolean isVelocityExceeded(String accountNumber) {
		String key = "fraud.velocity" + accountNumber;
		Long count = redisTemplate.opsForValue().increment(key); // 6 - flagged need to check the user

		if (count != null && count == 1) {
			redisTemplate.expire(key, Expiration.from(Duration.ofSeconds(60)));
		}

		log.info(
				"Velocity check - account {}, count: {}/{}",
				accountNumber,
				count,
				maxTransactionsPerMinute);

		return count != null && count > maxTransactionsPerMinute;
	}

	private boolean isAmountSuspicious(String accountNumber, BigDecimal amount) {
		String avgKey = "fraud.avg.amount" + accountNumber;
		String avgCountKey = "fraud.avg.count" + accountNumber;
		String avgStr = redisTemplate.opsForValue().get(avgKey);
		String avgCnt = redisTemplate.opsForValue().get(avgCountKey);

		if (avgStr == null || avgCnt == null) {
			redisTemplate.opsForValue().set(avgKey, amount.toString());
			redisTemplate.opsForValue().set(avgCountKey, "1");
			return false;
		}

		BigDecimal avgAmount = new BigDecimal(avgStr);
		BigDecimal avgCount = new BigDecimal(avgCnt);
		BigDecimal threshold = avgAmount.multiply(BigDecimal.valueOf(suspiciousAmountMultiplier));
		// update running average
		BigDecimal newAvg =
				avgAmount
						.multiply(avgCount)
						.add(amount)
						.divide(avgCount.add(BigDecimal.ONE), 2, RoundingMode.HALF_UP);
		redisTemplate.opsForValue().set(avgKey, newAvg.toString());
		redisTemplate.opsForValue().set(avgCountKey, (avgCount.add(BigDecimal.ONE)).toString());
		log.info(
				"Amount check - Amount {}, Threshold: {}, Suspicous: {}",
				avgAmount,
				threshold,
				amount.compareTo(threshold) > 0);
		return amount.compareTo(threshold) > 0;
	}
}
