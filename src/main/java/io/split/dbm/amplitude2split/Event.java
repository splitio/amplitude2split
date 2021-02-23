package io.split.dbm.amplitude2split;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Event {
    private static final SimpleDateFormat SERVER_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final SimpleDateFormat SERVER_FORMAT_2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private transient final JsonObject amplitudeEvent;
    private transient final Configuration config;

    // Fields for serialization by Gson
    private final String key;
    private final String eventTypeId;
    private final String trafficTypeName;
    private final String environmentName;
    private final Map<String, Object> properties;
    private final long timestamp;
    private final Double value;

    public static Optional<Event> fromJson(String json, Configuration config) {
        try {
            return Optional.of(new Event(json, config));
        } catch (IllegalStateException exception) {
            System.err.printf("WARN - Error parsing event: error=%s %n", exception.getMessage());
            return Optional.empty();
        }
    }

    public Event(String json, Configuration config) {
        this.amplitudeEvent = new Gson().fromJson(json, JsonObject.class);
        this.config = config;

        this.key = this.userId().orElseThrow(() -> new IllegalStateException("User ID is required."));
        this.timestamp = this.timestamp().orElseThrow(() -> new IllegalStateException("Event time is required."));
        this.value = this.value();
        this.eventTypeId = this.eventTypeId();
        this.properties = this.properties();
        this.trafficTypeName = config.splitTrafficType();
        this.environmentName = config.splitEnvironment();
    }

    public Optional<String> userId() {
        String userIdField = config.userIdField();
        if(!amplitudeEvent.has(userIdField)) {
            System.err.printf("WARN - User ID field not found for event: field=%s event=%s %n", userIdField, amplitudeEvent.toString());
            return Optional.empty();
        }
        return Optional.of(amplitudeEvent.get(userIdField).getAsString());
    }

    public Double value() {
        // Only get value if field is set
        String valueField = config.valueField();
        if(valueField != null && !valueField.isEmpty()) {
            try {
                return amplitudeEvent.get(valueField).getAsDouble();
            } catch (Exception exception) {
                System.err.printf("WARN - Event did not have a valid Value: key=%s %n", valueField);
            }
        }
        return null;
    }

    public String eventTypeId() {
        String eventTypeId = config.eventTypePrefix() + "null";
        if(amplitudeEvent.has("event_type")) {
            eventTypeId = config.eventTypePrefix() + amplitudeEvent.get("event_type").getAsString();
        }
        return eventTypeId;
    }

    public Optional<Long> timestamp() {
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

    public Map<String, Object> properties() {
        HashMap<String, Object> properties = new HashMap<>();

        for(String propertyKey : config.propertyFields()) {
            if(amplitudeEvent.has(propertyKey) && !amplitudeEvent.get(propertyKey).isJsonNull()) {
                properties.put(propertyKey, amplitudeEvent.get(propertyKey).getAsString());
            }
        }

        if(amplitudeEvent.has("user_properties")) {
            JsonObject userPropsObj = amplitudeEvent.getAsJsonObject("user_properties");
            String[] userPropsKeys = new String[] { "organizationPlatType", "role", "organizationName", "organizationSupportPlanType"};
            for(String userPropKey : userPropsKeys) {
                if(userPropsObj.has(userPropKey) && !userPropsObj.get(userPropKey).isJsonNull()) {
                    properties.put("user_properties." + userPropKey, userPropsObj.get(userPropKey).getAsString());
                }
            }
        }

        return properties;
    }
}
