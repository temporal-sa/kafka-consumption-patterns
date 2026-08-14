package io.temporal.samples.kafka.worker.chaos;

/**
 * Thrown by a simulated downstream service that is slow, flaky, or down.
 *
 * <p>An ordinary RuntimeException, so Temporal treats it as retryable and applies the activity's
 * retry policy. Nothing here is Temporal-specific: this is what a real client library failure looks
 * like, and the retry behaviour comes from the activity options rather than from the exception type.
 */
public class DownstreamUnavailableException extends RuntimeException {

  public DownstreamUnavailableException(String message) {
    super(message);
  }
}
