package com.waiting.bus.core.models;

import com.google.common.collect.Iterables;
import java.util.List;


public class Result {

  private final boolean successful;

  private final List<Attempt> reservedAttempts;

  private final int attemptCount;

  public Result(boolean successful, List<Attempt> reservedAttempts, int attemptCount) {
    this.successful = successful;
    this.reservedAttempts = reservedAttempts;
    this.attemptCount = attemptCount;
  }


  public boolean isSuccessful() {
    return successful;
  }


  public List<Attempt> getReservedAttempts() {
    return reservedAttempts;
  }


  public int getAttemptCount() {
    return attemptCount;
  }


  public String getErrorCode() {
    Attempt lastAttempt = Iterables.getLast(reservedAttempts);
    return lastAttempt.getErrorCode();
  }


  public String getErrorMessage() {
    Attempt lastAttempt = Iterables.getLast(reservedAttempts);
    return lastAttempt.getErrorMessage();
  }

  @Override
  public String toString() {
    return "Result{"
        + "successful="
        + successful
        + ", reservedAttempts="
        + reservedAttempts
        + ", attemptCount="
        + attemptCount
        + '}';
  }
}
