package io.temporal.samples.kafka.worker.chaos;

import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Runtime control over the simulated downstream failures.
 *
 * <p>The demo this exists for:
 *
 * <pre>
 *   curl -X POST localhost:8081/chaos/email/down     # provider goes offline
 *   # ...produce orders; watch workflows park in retry in the Web UI...
 *   curl -X POST localhost:8081/chaos/email/up       # provider returns
 *   # ...every parked workflow completes on its own, no events lost...
 * </pre>
 */
@RestController
@RequestMapping("/chaos")
public class ChaosController {

  /** Service names used by the activity implementations. */
  public static final String ORDER_DB = "orderDb";

  public static final String SHIPPING_DB = "shippingDb";
  public static final String INVOICE = "invoice";
  public static final String EMAIL = "email";

  private final ChaosProperties properties;

  public ChaosController(ChaosProperties properties) {
    this.properties = properties;
  }

  @GetMapping
  public Map<String, ChaosProperties.ServiceChaos> current() {
    // Touch the known services so an empty config still shows the full set.
    for (String s : new String[] {ORDER_DB, SHIPPING_DB, INVOICE, EMAIL}) {
      properties.forService(s);
    }
    return properties.getServices();
  }

  @PostMapping("/{service}")
  public ChaosProperties.ServiceChaos update(
      @PathVariable String service, @RequestBody ChaosUpdate update) {
    ChaosProperties.ServiceChaos chaos = properties.forService(service);
    if (update.down() != null) {
      chaos.setDown(update.down());
    }
    if (update.failureRate() != null) {
      chaos.setFailureRate(update.failureRate());
    }
    if (update.latencyMs() != null) {
      chaos.setLatencyMs(update.latencyMs());
    }
    return chaos;
  }

  @PostMapping("/{service}/down")
  public ChaosProperties.ServiceChaos down(@PathVariable String service) {
    ChaosProperties.ServiceChaos chaos = properties.forService(service);
    chaos.setDown(true);
    return chaos;
  }

  @PostMapping("/{service}/up")
  public ChaosProperties.ServiceChaos up(@PathVariable String service) {
    ChaosProperties.ServiceChaos chaos = properties.forService(service);
    chaos.setDown(false);
    return chaos;
  }

  @PostMapping("/reset")
  public Map<String, ChaosProperties.ServiceChaos> reset() {
    properties.getServices().clear();
    return current();
  }

  public record ChaosUpdate(Boolean down, Double failureRate, Long latencyMs) {}
}
