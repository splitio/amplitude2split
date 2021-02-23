package io.split.dbm.amplitude2split;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Set;

public class Configuration {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd'T'HH");

    private final Duration fetchWindow;
    private final Instant jobStart;

    @SerializedName("amplitude.api.key")
    private String amplitudeApiKey;
    @SerializedName("amplitude.api.secret")
    private String amplitudeApiSecret;
    @SerializedName("split.api.key")
    private String splitApiKey;
    @SerializedName("trafficType")
    private String splitTrafficType;
    @SerializedName("environment")
    private String splitEnvironment;
    @SerializedName("eventPrefix")
    private String eventTypePrefix;
    @SerializedName("key")
    private String userIdField;
    @SerializedName("value")
    private String valueField;
    @SerializedName("batchSize")
    private int batchSize;
    @SerializedName("string.property.keys")
    private Set<String> propertyFields;

    public static Configuration fromFile(String configFilePath) throws IOException {
        // Read Configuration
        byte[] encoded = Files.readAllBytes(Paths.get(configFilePath));
        String configContents = new String(encoded, Charset.defaultCharset());
        return new Gson().fromJson(configContents, Configuration.class);
    }

    public Configuration() {
        this.jobStart = Instant.now();
        this.fetchWindow = Duration.ofHours(1);
    }

    public String amplitudeApiKey() {
        return amplitudeApiKey;
    }

    public String amplitudeApiSecret() {
        return amplitudeApiSecret;
    }

    public String splitApiKey() {
        return splitApiKey;
    }

    public String splitTrafficType() {
        return splitTrafficType;
    }

    public String splitEnvironment() {
        return splitEnvironment;
    }

    public String eventTypePrefix() {
        return eventTypePrefix;
    }

    public String userIdField() {
        return userIdField;
    }

    public String valueField() {
        return valueField;
    }

    public int batchSize() {
        return batchSize;
    }

    public Set<String> propertyFields() {
        return propertyFields;
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
