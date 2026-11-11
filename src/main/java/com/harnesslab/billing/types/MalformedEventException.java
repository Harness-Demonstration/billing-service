package com.harnesslab.billing.types;

/** An incoming message does not match the contract. It is never retried; it goes to the dead-letter topic. */
public class MalformedEventException extends RuntimeException {
    public MalformedEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
