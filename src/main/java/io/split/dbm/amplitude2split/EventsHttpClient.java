package io.split.dbm.amplitude2split;

import com.google.gson.Gson;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

public class EventsHttpClient {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd'T'HH");
    private static final String AMPLITUDE_EVENTS_URL = "https://amplitude.com/api/2/export?start=%s&end=%s";
    private static final String SPLIT_EVENTS_URL = "https://events.split.io/api/events/bulk";

    private final Configuration config;
    private final HttpClient httpClient;

    public EventsHttpClient(Configuration config) {
        this.config = config;
        this.httpClient = HttpClient.newHttpClient();
    }

    public Stream<Event> getEventsFromAmplitude() throws IOException, InterruptedException {
        // Build Request
        String windowStart = DATE_FORMAT.format(Date.from(config.jobStart.minus(config.fetchWindow)));
        String windowEnd = DATE_FORMAT.format(Date.from(config.jobStart));
        URI uri = URI.create(String.format(AMPLITUDE_EVENTS_URL, windowStart, windowEnd));
        HttpRequest request =  HttpRequest.newBuilder(uri).GET()
                .header("Authorization", basicAuth(config.amplitudeApiKey, config.amplitudeApiSecret))
                .build();
        System.out.printf("INFO - Requesting Amplitude events: GET %s %n", uri);

        // Process response
        HttpResponse<InputStream> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofInputStream());
        if(response.statusCode() >= 300) {
            System.err.printf("ERROR - Amplitude events request failed: status=%s %n", response.statusCode());
            return Stream.empty();
        }
        return new EventResult(config, response.body()).stream();
    }

    public void sendEventsToSplitBatched(Stream<Event> events) {
        List<Event> batch = new LinkedList<>();
        events.forEach(event -> {
            batch.add(event);
            if(batch.size() >= config.batchSize) {
                sendEventsToSplit(batch);
                batch.clear();
            }
        });
        sendEventsToSplit(batch);
    }

    public void sendEventsToSplit(List<Event> events) {
        try {
            System.out.println("INFO - Sending batch of events: size=" + events.size());

            // Build Request
            HttpRequest request = HttpRequest.newBuilder(URI.create(SPLIT_EVENTS_URL))
                    .header("Content-type", "application/json")
                    .header("Authorization", "Bearer " + config.splitApiKey)
                    .POST(HttpRequest.BodyPublishers.ofString(new Gson().toJson(events)))
                    .build();

            // Process Response
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if(response.statusCode() >= 400) {
                System.err.printf("ERROR - Sending events to Split failed: status=%s response=%s %n", response.statusCode(), response.body());
            }

            // Courtesy to minimize pressure on API
            Thread.sleep(100);
        } catch (Exception exception) {
            System.err.println("ERROR - Failed to send event");
            exception.printStackTrace(System.err);
        }
    }

    private static String basicAuth(String username, String password) {
        return "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
    }
}
