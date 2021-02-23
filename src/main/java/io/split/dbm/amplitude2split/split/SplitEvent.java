package io.split.dbm.amplitude2split.split;

import io.split.dbm.amplitude2split.amplitude.AmplitudeEvent;

import java.util.Map;
import java.util.Optional;

public class SplitEvent {
    // Fields for serialization by Gson
    private final String key;
    private final String eventTypeId;
    private final String trafficTypeName;
    private final String environmentName;
    private final Map<String, Object> properties;
    private final long timestamp;
    private final Double value;

    public static Optional<SplitEvent> toSplitEvent(AmplitudeEvent event) {
        try {
            return Optional.of(new SplitEvent(event));
        } catch (IllegalStateException exception) {
            System.err.printf("WARN - Error parsing Split event: error=%s %n", exception.getMessage());
            return Optional.empty();
        }
    }

    public SplitEvent(AmplitudeEvent event) {
        this.key = event.userId().orElseThrow(() -> new IllegalStateException("User ID is required."));
        this.timestamp = event.timestamp().orElseThrow(() -> new IllegalStateException("Event time is required."));
        this.value = event.value();
        this.eventTypeId = event.eventTypeId();
        this.properties = event.properties();
        this.trafficTypeName = event.trafficType();
        this.environmentName = event.environment();
    }
}
