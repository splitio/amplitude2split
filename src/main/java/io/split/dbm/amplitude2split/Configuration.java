package io.split.dbm.amplitude2split;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class Configuration {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd'T'HH");

    public String amplitudeApiKey;
    public String amplitudeApiSecret;
    public String splitApiKey;
    public String splitTrafficType;
    public String splitEnvironment;
    public String eventTypePrefix;
    public String userIdField;
    public String valueField;
    public int batchSize;
    public Set<String> propertyFields;

    private final Duration fetchWindow;
    private final Instant jobStart;

    public Configuration() {
        this.jobStart = Instant.now();
        this.fetchWindow = Duration.ofHours(1);
    }

    public String jobStartTime() {
        return DATE_FORMAT.format(Date.from(jobStart));
    }

    public long jobElapsedTime() {
        return Duration.between(jobStart, Instant.now()).getSeconds();
    }

    public String windowStart() {
        return DATE_FORMAT.format(Date.from(jobStart.minus(fetchWindow)));
    }

    public String windowEnd() {
        return DATE_FORMAT.format(Date.from(jobStart));
    }

    public static Optional<Configuration> fromFile(String configFilePath) {
        try {
            // Read Configuration
            byte[] encoded = Files.readAllBytes(Paths.get(configFilePath));
            String configContents = new String(encoded, Charset.defaultCharset());
            JSONObject configObj = new JSONObject(configContents);

            // Build Configuration
            Configuration config = new Configuration();
            config.amplitudeApiKey = configObj.getString("amplitude.api.key");
            config.amplitudeApiSecret = configObj.getString("amplitude.api.secret");
            config.splitApiKey = configObj.getString("split.api.key");
            config.splitTrafficType = configObj.getString("trafficType");
            config.splitEnvironment = configObj.getString("environment");
            config.eventTypePrefix = configObj.getString("eventPrefix");
            config.batchSize = configObj.getInt("batchSize");
            config.userIdField = configObj.getString("key");
            config.valueField = configObj.getString("value");

            JSONArray keysArray = configObj.getJSONArray("string.property.keys");
            config.propertyFields = new HashSet<>();
            for(int i = 0; i < keysArray.length(); i++) {
                config.propertyFields.add(keysArray.getString(i));
            }

            return Optional.of(config);
        } catch (IOException exception) {
            System.err.println("Error reading configuration file: " + configFilePath);
            return Optional.empty();
        }
    }
}
