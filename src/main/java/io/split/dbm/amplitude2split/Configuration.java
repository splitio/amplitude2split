package io.split.dbm.amplitude2split;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;

public class Configuration {
    public final Instant jobStart = Instant.now();
    public final Duration fetchWindow = Duration.ofHours(1);

    @SerializedName("amplitude.api.key") public String amplitudeApiKey;
    @SerializedName("amplitude.api.secret") public String amplitudeApiSecret;
    @SerializedName("split.api.key") public String splitApiKey;
    @SerializedName("trafficType") public String splitTrafficType;
    @SerializedName("environment") public String splitEnvironment;
    @SerializedName("eventPrefix") public String eventTypePrefix;
    @SerializedName("key") public String userIdField;
    @SerializedName("value") public String valueField;
    @SerializedName("batchSize") public int batchSize;
    @SerializedName("string.property.keys") public Set<String> propertyFields;

    public static Configuration fromFile(String configFilePath) throws IOException {
        String configContents = Files.readString(Paths.get(configFilePath));
        return new Gson().fromJson(configContents, Configuration.class);
    }
}
