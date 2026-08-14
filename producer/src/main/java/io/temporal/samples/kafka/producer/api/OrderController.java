package io.temporal.samples.kafka.producer.api;

import io.temporal.samples.kafka.common.model.OrderCompleted;
import io.temporal.samples.kafka.producer.config.ProducerProperties;
import io.temporal.samples.kafka.producer.publish.OrderPublisher;
import io.temporal.samples.kafka.producer.publish.StreamingPublisher;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

  private final OrderPublisher publisher;
  private final StreamingPublisher streaming;
  private final ProducerProperties properties;

  public OrderController(
      OrderPublisher publisher, StreamingPublisher streaming, ProducerProperties properties) {
    this.publisher = publisher;
    this.streaming = streaming;
    this.properties = properties;
  }

  /** Publishes one event. Send a body to control the payload, or omit it to generate one. */
  @PostMapping
  public OrderPublisher.PublishOutcome publishOne(
      @RequestBody(required = false) OrderCompleted body) {
    return body != null ? publisher.publish(body) : publisher.publishNext();
  }

  @PostMapping("/batch")
  public List<OrderPublisher.PublishOutcome> publishBatch(
      @RequestParam(defaultValue = "10") int count) {
    List<OrderPublisher.PublishOutcome> results = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      results.add(publisher.publishNext());
    }
    return results;
  }

  @PostMapping("/stream")
  public Map<String, Object> startStream(@RequestParam(required = false) Integer ratePerSecond) {
    streaming.start(
        ratePerSecond != null ? ratePerSecond : properties.getDefaultRatePerSecond());
    return streamState();
  }

  @DeleteMapping("/stream")
  public Map<String, Object> stopStream() {
    streaming.stop();
    return streamState();
  }

  @GetMapping("/stream")
  public Map<String, Object> streamState() {
    return Map.of(
        "running", streaming.isRunning(),
        "ratePerSecond", streaming.getRatePerSecond(),
        "totalPublished", streaming.getTotalPublished(),
        "topic", properties.getTopic(),
        "partitions", properties.getPartitions(),
        "duplicateRate", properties.getDuplicateRate(),
        "malformedRate", properties.getMalformedRate());
  }

  /** Adjusts the injection rates at runtime, so a demo doesn't need a restart. */
  @PostMapping("/injection")
  public Map<String, Object> setInjection(
      @RequestParam(required = false) Double duplicateRate,
      @RequestParam(required = false) Double malformedRate) {
    if (duplicateRate != null) {
      properties.setDuplicateRate(duplicateRate);
    }
    if (malformedRate != null) {
      properties.setMalformedRate(malformedRate);
    }
    return streamState();
  }
}
