package com.waiting.bus.exceptions;

import com.waiting.bus.core.models.Attempt;
import com.waiting.bus.core.models.Result;

import java.util.List;

public class ResultFailedException extends ProducerException {

  private final Result result;

  public ResultFailedException(Result result) {
    this.result = result;
  }

  public Result getResult() {
    return result;
  }

  public String getErrorCode() {
    return result.getErrorCode();
  }

  public String getErrorMessage() {
    return result.getErrorMessage();
  }

  public List<Attempt> getReservedAttempts() {
    return result.getReservedAttempts();
  }
}
