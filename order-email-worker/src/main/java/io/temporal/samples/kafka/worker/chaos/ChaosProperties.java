package io.temporal.samples.kafka.worker.chaos;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Failure-injection knobs for the simulated downstream services.
 *
 * <p>Mutable at runtime (see {@code ChaosController}) so a demo can take the email provider down
 * mid-run, show workflows parked in retry with their full history visible, bring it back, and show
 * every one of them complete without intervention.
 */
@ConfigurationProperties(prefix = "app.chaos")
public class ChaosProperties {

  private final Map<String, ServiceChaos> services = new ConcurrentHashMap<>();

  public Map<String, ServiceChaos> getServices() {
    return services;
  }

  public ServiceChaos forService(String name) {
    return services.computeIfAbsent(name, k -> new ServiceChaos());
  }

  public static class ServiceChaos {

    /** Hard outage: every call fails until turned off. */
    private boolean down;

    /** Probability in [0,1] that any given call fails transiently. */
    private double failureRate;

    /** Artificial delay applied before the call succeeds or fails. */
    private long latencyMs;

    public boolean isDown() {
      return down;
    }

    public void setDown(boolean down) {
      this.down = down;
    }

    public double getFailureRate() {
      return failureRate;
    }

    public void setFailureRate(double failureRate) {
      this.failureRate = failureRate;
    }

    public long getLatencyMs() {
      return latencyMs;
    }

    public void setLatencyMs(long latencyMs) {
      this.latencyMs = latencyMs;
    }
  }
}
