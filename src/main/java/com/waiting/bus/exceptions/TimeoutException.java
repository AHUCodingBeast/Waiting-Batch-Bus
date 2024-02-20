package com.waiting.bus.exceptions;

public class TimeoutException extends ProducerException {

  public TimeoutException() {
    super();
  }

  public TimeoutException(String message, Throwable cause) {
    super(message, cause);
  }

  public TimeoutException(String message) {
    super(message);
  }

  public TimeoutException(Throwable cause) {
    super(cause);
  }
}
