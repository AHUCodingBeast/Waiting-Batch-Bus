package com.waiting.bus.exceptions;


public class ProducerException extends Exception {

  public ProducerException(String message, Throwable cause) {
    super(message, cause);
  }

  public ProducerException(String message) {
    super(message);
  }

  public ProducerException(Throwable cause) {
    super(cause);
  }

  public ProducerException() {
    super();
  }
}
