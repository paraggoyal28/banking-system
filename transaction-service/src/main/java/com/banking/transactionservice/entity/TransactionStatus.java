package com.banking.transactionservice.entity;

/*
 * Transaction lifecycle flow
 * Pending - Processing - Completed (Clean transaction)
 * Pending - Processing - Pending Verification(Suspicious Detected) - (Verified) COMPLETED -
 * (not verified) FLAGGER (saga refund) - FAILED
 */
public enum TransactionStatus {
  PENDING,
  PROCESSING,
  PENDING_VERIFICATION,
  COMPLETED,
  FAILED,
  FLAGGED
}
