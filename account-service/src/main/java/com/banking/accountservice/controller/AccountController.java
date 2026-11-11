package com.banking.accountservice.controller;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.service.AccountService;
import com.banking.accountservice.dto.CreateAccountRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;

@RestController
@RequestMapping("/api/v1/accounts")
@Slf4j
@RequiredArgsConstructor
public class AccountController {

  private final AccountService accountService;

  @PostMapping
  public ResponseEntity<AccountResponse> createAccount(
      @Valid @RequestBody CreateAccountRequest request) {
    return ResponseEntity.status(HttpStatus.CREATED).body(accountService.createAccount(request));
  }

  @GetMapping("/{accountNumber}")
  public ResponseEntity<AccountResponse> getAccount(@PathVariable String accountNumber) {
    return ResponseEntity.ok(accountService.getAccount(accountNumber));
  }

  @GetMapping("/{accountNumber}/balance")
  public ResponseEntity<BigDecimal> getBalance(@PathVariable String accountNumber) {
    return ResponseEntity.ok(accountService.getBalance(accountNumber));
  }

  @PutMapping("/{accountNumber}/block")
  public ResponseEntity<String> blockAccount(@PathVariable String accountNumber) {
    accountService.block(accountNumber);
    return ResponseEntity.ok("Account blocked successfully");
  }

  /*
  SAGA Step 1
  Deduct balance
  Called by transaction service when transfer is initiated
   */

  @PutMapping("/{accountNumber}/deduct")
  public ResponseEntity<String> deductBalance(
      @PathVariable String accountNumber, @RequestParam BigDecimal amount) {
    accountService.deductBalance(accountNumber, amount);
    return ResponseEntity.ok("Balance deducted successfully");
  }

  /*
  SAGA Step 4
  Compensating transaction endpoint
  Called by transaction service in two scenarios
  1. Fraud detected - refund sender (undo step 1)
  2. transaction completed - credit receiver
   */
  @PutMapping("/{accountNumber}/credit")
  public ResponseEntity<String> creditBalance(
      @PathVariable String accountNumber, @RequestParam BigDecimal amount) {
    accountService.creditBalance(accountNumber, amount);
    return ResponseEntity.ok("Balance credited successfully.");
  }
}
