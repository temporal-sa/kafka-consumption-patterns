package io.temporal.samples.kafka.producer.publish;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Continuous event generation at a target rate, adjustable without restarting (PRD FR-P3).
 *
 * <p>Publishes on a fixed 100 ms tick rather than scheduling one task per event: at a few hundred
 * events/second, per-event scheduling jitter would dominate the measurement. Fractional per-tick
 * counts are carried over so that, say, 15/s produces 15 events per second rather than 10 or 20.
 *
 * <p>This is the primary knob for the load tests, including the partition-ceiling experiment.
 */
@Component
public class StreamingPublisher {

  private static final Logger log = LoggerFactory.getLogger(StreamingPublisher.class);
  private static final long TICK_MS = 100;

  private final OrderPublisher publisher;
  private final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor(
          r -> {
            Thread t = new Thread(r, "order-stream");
            t.setDaemon(true);
            return t;
          });

  private final AtomicLong totalPublished = new AtomicLong();
  private volatile ScheduledFuture<?> task;
  private volatile int ratePerSecond;
  private double carry;

  public StreamingPublisher(OrderPublisher publisher) {
    this.publisher = publisher;
  }

  public synchronized void start(int ratePerSecond) {
    if (ratePerSecond <= 0) {
      throw new IllegalArgumentException("ratePerSecond must be > 0");
    }
    stop();
    this.ratePerSecond = ratePerSecond;
    this.carry = 0;
    this.task = scheduler.scheduleAtFixedRate(this::tick, 0, TICK_MS, TimeUnit.MILLISECONDS);
    log.info("Streaming started at {} events/sec", ratePerSecond);
  }

  public synchronized void stop() {
    if (task != null) {
      task.cancel(false);
      task = null;
      log.info("Streaming stopped after {} events", totalPublished.get());
    }
    ratePerSecond = 0;
  }

  public boolean isRunning() {
    return task != null;
  }

  public int getRatePerSecond() {
    return ratePerSecond;
  }

  public long getTotalPublished() {
    return totalPublished.get();
  }

  private void tick() {
    try {
      double exact = (ratePerSecond * TICK_MS / 1000.0) + carry;
      int toSend = (int) Math.floor(exact);
      carry = exact - toSend;
      for (int i = 0; i < toSend; i++) {
        publisher.publishNext();
        totalPublished.incrementAndGet();
      }
    } catch (RuntimeException e) {
      // Never let an exception kill the scheduled task — a broker hiccup should pause
      // publishing, not silently end the run and skew a load test.
      log.error("Publish tick failed; continuing", e);
    }
  }

  @PreDestroy
  public void shutdown() {
    stop();
    scheduler.shutdownNow();
  }
}
