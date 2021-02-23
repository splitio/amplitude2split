package io.split.dbm.amplitude2split.amplitude;

import io.split.dbm.amplitude2split.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;
import java.util.stream.Stream;

public class AmplitudeEventsClient {
    private final Configuration config;

    public AmplitudeEventsClient(Configuration config) {
        this.config = config;
    }

    public Stream<AmplitudeEvent> getEvents() throws IOException, InterruptedException {
        Optional<AmplitudeEventResult> result = requestEvents();
        return result.map(AmplitudeEventResult::stream)
                .orElse(Stream.empty());
    }

    private Optional<AmplitudeEventResult> requestEvents() throws IOException, InterruptedException {
        HttpClient client = client();
        HttpRequest request = eventsRequest();
        HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());

        int statusCode = response.statusCode();
        if(statusCode >= 300) {
            System.err.println("ERROR - Amplitude events request failed: status=" + statusCode);
            return Optional.empty();
        }
        return Optional.of(new AmplitudeEventResult(config, response.body()));
    }

    private HttpClient client() {
        return HttpClient.newBuilder()
                .authenticator(new Authenticator() {
                    @Override
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(config.amplitudeApiKey, config.amplitudeApiSecret.toCharArray());
                    }
                })
                .build();
    }

    private HttpRequest eventsRequest() {
        String start = config.windowStart();
        String end = config.windowEnd();
        String uri = "https://amplitude.com/api/2/export?start=" + start + "&end=" + end;

        System.out.println("INFO - GET " + uri);

        return HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(uri))
                .build();
    }
}
