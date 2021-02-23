package io.split.dbm.amplitude2split.amplitude;

import io.split.dbm.amplitude2split.Configuration;
import org.json.JSONException;
import org.json.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class AmplitudeEvent {
    private static final SimpleDateFormat SERVER_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final SimpleDateFormat SERVER_FORMAT_2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private final JSONObject amplitudeEvent;
    private final Configuration config;

    public AmplitudeEvent(String line, Configuration config) {
        this.amplitudeEvent = new JSONObject(line);
        this.config = config;
    }

    public Optional<String> userId() {
        String userIdField = config.userIdField;

        String userId = "";
        if(amplitudeEvent.has(userIdField)) {
            Object userObj = amplitudeEvent.get(userIdField);
            if(userObj instanceof String) {
                userId = amplitudeEvent.getString(userIdField);
            } else {
                System.err.println("WARN - " + userIdField + " is not a string: " + userObj.toString());
                userId = userObj.toString();
            }
        }
        if(userId.isEmpty()) {
            System.err.println("WARN - " + userIdField + " not found for event: " + amplitudeEvent.toString(2));
            return Optional.empty();
        }
        return Optional.of(userId);
    }

    public Optional<Double> value() {
        // Only get value if field is set
        if(config.valueField != null && !config.valueField.isEmpty()) {
            try {
                return Optional.of(amplitudeEvent.getDouble(config.valueField));
            } catch (JSONException exception) {
                System.err.println("WARN - Event did not have a valid Value: key=" + config.valueField);
            }
        }
        return Optional.empty();
    }

    public String eventTypeId() {
        String eventTypeId = config.eventTypePrefix + "null";
        if(amplitudeEvent.has("event_type")) {
            eventTypeId = config.eventTypePrefix + amplitudeEvent.getString("event_type");
        }
        return eventTypeId;
    }

    public Optional<Long> timestamp() {
        String serverUploadTime = amplitudeEvent.getString("event_time");
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

        for(String propertyKey : config.propertyFields) {
            if(amplitudeEvent.has(propertyKey) && !amplitudeEvent.isNull(propertyKey)) {
                properties.put(propertyKey, amplitudeEvent.getString(propertyKey));
            }
        }

        if(amplitudeEvent.has("user_properties")) {
            JSONObject userPropsObj = amplitudeEvent.getJSONObject("user_properties");
            String[] userPropsKeys = new String[] { "organizationPlatType", "role", "organizationName", "organizationSupportPlanType"};
            for(String userPropKey : userPropsKeys) {
                if(userPropsObj.has(userPropKey) && !userPropsObj.isNull(userPropKey)) {
                    properties.put("user_properties." + userPropKey, userPropsObj.getString(userPropKey));
                }
            }
        }

        return properties;
    }
}
