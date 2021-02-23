package io.split.dbm.amplitude2split.amplitude;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.split.dbm.amplitude2split.Configuration;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class AmplitudeEvent {
    private static final SimpleDateFormat SERVER_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final SimpleDateFormat SERVER_FORMAT_2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private final JsonObject amplitudeEvent;
    private final Configuration config;

    public AmplitudeEvent(String line, Configuration config) {
        this.amplitudeEvent = new Gson().fromJson(line, JsonObject.class);
        this.config = config;
    }

    public String trafficType() {
        return config.splitTrafficType();
    }

    public String environment() {
        return config.splitEnvironment();
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
