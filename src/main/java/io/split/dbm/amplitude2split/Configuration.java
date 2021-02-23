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

    public static Configuration fromFile(String configFilePath) throws IOException {
        String configContents = Files.readString(Paths.get(configFilePath));
        return new Gson().fromJson(configContents, Configuration.class);
    }
}
