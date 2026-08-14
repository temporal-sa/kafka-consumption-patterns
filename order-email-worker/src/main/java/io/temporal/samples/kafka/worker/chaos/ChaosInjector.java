package io.temporal.samples.kafka.worker.chaos;

import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class ChaosInjector {

  private static final Logger log = LoggerFactory.getLogger(ChaosInjector.class);

  private final ChaosProperties properties;

  public ChaosInjector(ChaosProperties properties) {
    this.properties = properties;
  }

  /** Applies configured latency, then fails if the service is down or unlucky. */
  public void apply(String service) {
    ChaosProperties.ServiceChaos chaos = properties.forService(service);

    if (chaos.getLatencyMs() > 0) {
      sleep(chaos.getLatencyMs());
    }

    if (chaos.isDown()) {
      log.warn("[chaos] {} is DOWN — failing call", service);
      throw new DownstreamUnavailableException(service + " is unavailable");
    }

    if (chaos.getFailureRate() > 0
        && ThreadLocalRandom.current().nextDouble() < chaos.getFailureRate()) {
      log.warn("[chaos] {} transient failure (rate={})", service, chaos.getFailureRate());
      throw new DownstreamUnavailableException(service + " transient failure");
    }
  }

  private void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DownstreamUnavailableException("interrupted while simulating latency");
    }
  }
}
