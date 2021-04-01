package io.split.dbm.amplitude2split;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class Event {
    private static final SimpleDateFormat SERVER_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final SimpleDateFormat SERVER_FORMAT_2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public final String key;
    public final String eventTypeId;
    public final String trafficTypeName;
    public final String environmentName;
    public final Map<String, Object> properties;
    public final long timestamp;
    public final Double value;

    public Event(JsonObject amplitudeEvent, Configuration config) {
        this.key = userId(amplitudeEvent, config).orElseThrow(() -> new IllegalStateException("User ID is required."));
        this.timestamp = timestamp(amplitudeEvent).orElseThrow(() -> new IllegalStateException("Event time is required."));
        this.value = value(amplitudeEvent, config);
        this.eventTypeId = eventTypeId(amplitudeEvent, config);
        this.properties = properties(amplitudeEvent, config);
        this.trafficTypeName = config.splitTrafficType;
        this.environmentName = config.splitEnvironment;
    }

    public static Optional<Event> fromJson(String json, Configuration config) {
        try {
            JsonObject amplitudeEvent = new Gson().fromJson(json, JsonObject.class);
            return Optional.of(new Event(amplitudeEvent, config));
        } catch (IllegalStateException exception) {
            System.err.printf("WARN - Error parsing event: error=%s %n", exception.getMessage());
            return Optional.empty();
        }
    }

    private static Optional<String> userId(JsonObject amplitudeEvent, Configuration config) {
        String userIdField = config.userIdField;
        if(!amplitudeEvent.has(userIdField)) {
            System.err.printf("WARN - User ID field not found for event: field=%s event=%s %n", userIdField, amplitudeEvent.toString());
            return Optional.empty();
        }
        return Optional.of(amplitudeEvent.get(userIdField).toString().replace("\"", ""));
    }

    public static Double value(JsonObject amplitudeEvent, Configuration config) {
        // Only get value if field is set
        String valueField = config.valueField;
        if(valueField != null && !valueField.isEmpty()) {
            try {
                return amplitudeEvent.get(valueField).getAsDouble();
            } catch (Exception exception) {
                System.err.printf("WARN - Event did not have a valid Value: key=%s %n", valueField);
            }
        }
        return null;
    }

    public static String eventTypeId(JsonObject amplitudeEvent, Configuration config) {
        if(amplitudeEvent.has("event_type")) {
            return config.eventTypePrefix + amplitudeEvent.get("event_type").getAsString();
        }
        return config.eventTypePrefix + "null";
    }

    public static Optional<Long> timestamp(JsonObject amplitudeEvent) {
        String serverUploadTime = amplitudeEvent.get("event_time").getAsString();
        Date parsedServerTime;
        try {
            parsedServerTime = SERVER_FORMAT.parse(serverUploadTime);
        } catch (ParseException pe) {
            try {
                parsedServerTime = SERVER_FORMAT_2.parse(serverUploadTime);
            } catch (ParseException e) {
                System.err.println("ERROR - event_time could not be parsed");
                return Optional.empty();
            }
        }

        return Optional.of(parsedServerTime.getTime());
    }

    public static Map<String, Object> properties(JsonObject amplitudeEvent, Configuration config) {
        JsonObject userPropsObj = amplitudeEvent.getAsJsonObject("user_properties");

        HashMap<String, Object> properties = new HashMap<>();
        for(String propertyKey : config.propertyFields) {
            // Check Base Event
            if(amplitudeEvent.has(propertyKey) && !amplitudeEvent.get(propertyKey).isJsonNull()) {
                properties.put(propertyKey, amplitudeEvent.get(propertyKey).getAsString());
            }
            // Check User Properties
            if(userPropsObj != null && userPropsObj.has(propertyKey) && !userPropsObj.get(propertyKey).isJsonNull()) {
                properties.put(propertyKey, userPropsObj.get(propertyKey).getAsString());
            }
        }

        return properties;
    }
}
