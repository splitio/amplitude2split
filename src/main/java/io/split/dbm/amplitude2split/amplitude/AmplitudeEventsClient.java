package io.split.dbm.amplitude2split.amplitude;

import io.split.dbm.amplitude2split.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.stream.Stream;

public class AmplitudeEventsClient {
    private static final String EVENTS_URL = "https://amplitude.com/api/2/export?start=%s&end=%s";

    private final Configuration config;

    public AmplitudeEventsClient(Configuration config) {
        this.config = config;
    }

    public Stream<AmplitudeEvent> getEvents() throws IOException, InterruptedException {
        // Build Request
        URI uri = URI.create(String.format(EVENTS_URL, config.windowStart(), config.windowEnd()));
        HttpRequest request =  HttpRequest.newBuilder(uri).GET()
                .header("Authorization", basicAuth(config.amplitudeApiKey(), config.amplitudeApiSecret()))
                .build();
        System.out.printf("INFO - Requesting Amplitude events: GET %s %n", uri);

        // Process response
        HttpResponse<InputStream> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofInputStream());
        if(response.statusCode() >= 300) {
            System.err.printf("ERROR - Amplitude events request failed: status=%s %n", response.statusCode());
            return Stream.empty();
        }
        return new AmplitudeEventResult(config, response.body()).stream();
    }

    private static String basicAuth(String username, String password) {
        return "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
    }
}
