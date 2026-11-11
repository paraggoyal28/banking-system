package com.banking.accountservice.service;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.entity.Account;
import com.banking.accountservice.entity.AccountStatus;
import com.banking.accountservice.entity.AccountType;
import com.banking.accountservice.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.security.SecureRandom;
import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountService {

  private static SecureRandom secureRandom = new SecureRandom();
  private final AccountRepository accountRepository;

  public AccountResponse createAccount(CreateAccountRequest request) {
    log.info("Creating account for: {}", request.getEmail());

    if (accountRepository.existsByEmail(request.getEmail())) {
      throw new RuntimeException("Account already exists for email: " + request.getEmail());
    }

    Account account =
        Account.builder()
            .accountHolderName(request.getAccountHolderName())
            .email(request.getEmail())
            .balance(request.getInitialDeposit())
            .phone(request.getPhone())
            .accountType(request.getAccountType())
            .accountStatus(AccountStatus.ACTIVE)
            .accountNumber(generateAccountNumber())
            .dailyTransactionLimit(
                request.getAccountType().equals(AccountType.SAVINGS)
                    ? new BigDecimal("100000")
                    : new BigDecimal("500000")) // savings - 100000, current - 50000, fd - 50000
            .build();

    Account savedAccount = accountRepository.save(account);
    log.info("Account created: {}", savedAccount.getAccountNumber());
    return mapToResponse(savedAccount);
  }

  private String generateAccountNumber() {
    String accountNumber;

    do {
      long number = secureRandom.nextLong(1_000_000_000_000L);

      accountNumber = String.format("%012d", number);

    } while (accountRepository.existsByAccountNumber(accountNumber));

    return accountNumber;
  }

  private AccountResponse mapToResponse(Account savedAccount) {
    AccountResponse accountResponse = new AccountResponse();
    accountResponse.setId(savedAccount.getId());
    accountResponse.setAccountHolderName(savedAccount.getAccountHolderName());
    accountResponse.setAccountNumber(savedAccount.getAccountNumber());
    accountResponse.setAccountStatus(savedAccount.getAccountStatus());
    accountResponse.setBalance(savedAccount.getBalance());
    accountResponse.setDailyTransactionLimit(savedAccount.getDailyTransactionLimit());
    accountResponse.setCreatedAt(savedAccount.getCreatedAt());
    accountResponse.setEmail(savedAccount.getEmail());
    accountResponse.setPhone(savedAccount.getPhone());
    accountResponse.setAccountType(savedAccount.getAccountType());
    return accountResponse;
  }

  /** - getAccount by account number */
  public AccountResponse getAccount(String accountNumber) {
    log.info("Retrieving account for: {}", accountNumber);
    Account account =
        accountRepository
            .findByAccountNumber(accountNumber)
            .orElseThrow(() -> new RuntimeException("Account not found"));
    return mapToResponse(account);
  }

  /** - get balance for an account */
  public BigDecimal getBalance(String accountNumber) {
    log.info("Finding balance for account: {}", accountNumber);
    Account account =
        accountRepository
            .findByAccountNumber(accountNumber)
            .orElseThrow(() -> new RuntimeException("Account not found"));
    return account.getBalance();
  }

  /** block account - called by fraud detection service via Kafka */
  public void block(String accountNumber) {
    log.info("Blocking account: {}", accountNumber);
    Account account =
        accountRepository
            .findByAccountNumber(accountNumber)
            .orElseThrow(() -> new RuntimeException("Account not found"));
    account.setAccountStatus(AccountStatus.BLOCKED);
    accountRepository.save(account);
    log.info("Account Blocked: {}", accountNumber);
  }

  /* Deduct balance amount from sender accountNumber
   * @param accountNumber
   * @param amount
   * called by transaction service
   *
   */
  public void deductBalance(String accountNumber, BigDecimal amount) {
    log.info("Deducting Balance {} from account: {}", amount, accountNumber);
    Account account =
        accountRepository
            .findByAccountNumber(accountNumber)
            .orElseThrow(() -> new RuntimeException("Account not found"));

    if (account.getAccountStatus() != AccountStatus.ACTIVE) {
      throw new RuntimeException("Account not active: " + accountNumber);
    }

    if (account.getBalance().compareTo(amount) < 0) {
      throw new RuntimeException("Insufficient funds for account");
    }

    account.setBalance(account.getBalance().subtract(amount));
    accountRepository.save(account);

    log.info("Balance updated. New Balance is {}", account.getBalance());
  }

  /*
   * Credit balance -
   * Called by transaction service via kafka
   * @param -accountNumber
   * @param -amount
   */
  public void creditBalance(String accountNumber, BigDecimal amount) {
    log.info("Crediting Balance {} to account: {}", amount, accountNumber);

    Account account =
        accountRepository
            .findByAccountNumber(accountNumber)
            .orElseThrow(() -> new RuntimeException("Account not found"));

    account.setBalance(account.getBalance().add(amount));
    accountRepository.save(account);

    log.info("Balance Credited: New Balance: {}", account.getBalance());
  }
}
