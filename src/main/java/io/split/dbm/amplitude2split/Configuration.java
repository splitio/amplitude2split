package io.split.dbm.amplitude2split;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Set;

import com.google.gson.Gson;

public class Configuration {
    public final Instant jobStart = Instant.now();
    
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
    public int durationInHours;
    
    public static Configuration fromFile(String configFilePath) throws IOException {
        String configContents = Files.readString(Paths.get(configFilePath));
        return new Gson().fromJson(configContents, Configuration.class);
    }
}
