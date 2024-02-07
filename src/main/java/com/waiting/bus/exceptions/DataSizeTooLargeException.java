package com.waiting.bus.exceptions;

/** The log's size is larger than the maximum allowable size. */
public class DataSizeTooLargeException extends ProducerException {

  public DataSizeTooLargeException() {
    super();
  }

  public DataSizeTooLargeException(String message, Throwable cause) {
    super(message, cause);
  }

  public DataSizeTooLargeException(String message) {
    super(message);
  }

  public DataSizeTooLargeException(Throwable cause) {
    super(cause);
  }
}
