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

    public Configuration(String configFilePath) throws IOException {
        this.jobStart = Instant.now();
        this.fetchWindow = Duration.ofHours(1);

        // Read Configuration
        byte[] encoded = Files.readAllBytes(Paths.get(configFilePath));
        String configContents = new String(encoded, Charset.defaultCharset());
        JSONObject configObj = new JSONObject(configContents);

        // Build Configuration
        this.amplitudeApiKey = configObj.getString("amplitude.api.key");
        this.amplitudeApiSecret = configObj.getString("amplitude.api.secret");
        this.splitApiKey = configObj.getString("split.api.key");
        this.splitTrafficType = configObj.getString("trafficType");
        this.splitEnvironment = configObj.getString("environment");
        this.eventTypePrefix = configObj.getString("eventPrefix");
        this.batchSize = configObj.getInt("batchSize");
        this.userIdField = configObj.getString("key");
        this.valueField = configObj.getString("value");

        JSONArray keysArray = configObj.getJSONArray("string.property.keys");
        this.propertyFields = new HashSet<>();
        for(int i = 0; i < keysArray.length(); i++) {
            this.propertyFields.add(keysArray.getString(i));
        }
    }

    public String jobStartTime() {
        return jobStart.toString();
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
}
