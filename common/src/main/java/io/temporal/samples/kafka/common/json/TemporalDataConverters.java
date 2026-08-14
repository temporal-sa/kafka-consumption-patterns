package io.temporal.samples.kafka.common.json;

import io.temporal.common.converter.DataConverter;
import io.temporal.common.converter.DefaultDataConverter;
import io.temporal.common.converter.JacksonJsonPayloadConverter;

/**
 * Temporal {@link DataConverter} sharing the {@link Json} Jackson setup.
 *
 * <p>Every Spring app in this repo exposes this as a {@code DataConverter} bean; the Temporal Spring
 * Boot starter picks it up automatically. Workflow arguments must serialize identically in the
 * process that <em>starts</em> a workflow and the process that <em>runs</em> it, so all modules use
 * this one factory rather than each configuring its own.
 */
public final class TemporalDataConverters {

  public static DataConverter withJavaTime() {
    return DefaultDataConverter.newDefaultInstance()
        .withPayloadConverterOverrides(new JacksonJsonPayloadConverter(Json.newObjectMapper()));
  }

  private TemporalDataConverters() {}
}
