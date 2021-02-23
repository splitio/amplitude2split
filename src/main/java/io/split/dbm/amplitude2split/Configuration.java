package io.split.dbm.amplitude2split;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class Configuration {
    public String AMPLITUDE_API_TOKEN;
    public String AMPLITUDE_API_SECRET;
    public String SPLIT_API_TOKEN;
    public String SPLIT_TRAFFIC_TYPE;
    public String SPLIT_ENVIRONMENT;
    public String AMPLITUDE_PREFIX;
    public String USER_ID;
    public String VALUE_KEY;
    public int BATCH_SIZE;
    public Set<String> STRING_PROPERTY_KEYS;
    public long jobStart;

    public Configuration() {
        jobStart = System.currentTimeMillis();
    }

    public static Optional<Configuration> fromFile(String configFilePath) {
        try {
            Configuration config = new Configuration();

            String configFile = readFile(configFilePath);
            JSONObject configObj = new JSONObject(configFile);
            config.AMPLITUDE_API_TOKEN = configObj.getString("amplitude.api.key");
            config.AMPLITUDE_API_SECRET = configObj.getString("amplitude.api.secret");
            config.SPLIT_API_TOKEN = configObj.getString("split.api.key");
            config.SPLIT_TRAFFIC_TYPE = configObj.getString("trafficType");
            config.SPLIT_ENVIRONMENT = configObj.getString("environment");
            config.AMPLITUDE_PREFIX = configObj.getString("eventPrefix");
            config.BATCH_SIZE = configObj.getInt("batchSize");
            config.USER_ID = configObj.getString("key");
            config.VALUE_KEY = configObj.getString("value");

            JSONArray keysArray = configObj.getJSONArray("string.property.keys");
            config.STRING_PROPERTY_KEYS = new HashSet<>();
            for(int i = 0; i < keysArray.length(); i++) {
                config.STRING_PROPERTY_KEYS.add(keysArray.getString(i));
            }

            return Optional.of(config);
        } catch (IOException exception) {
            System.err.println("Error reading configuration file: " + configFilePath);
            return Optional.empty();
        }
    }

    public static String readFile(String path) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, Charset.defaultCharset());
    }
}
