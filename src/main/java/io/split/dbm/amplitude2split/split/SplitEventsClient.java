package io.split.dbm.amplitude2split.split;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import com.google.gson.Gson;
import io.split.dbm.amplitude2split.Configuration;

public class SplitEventsClient {
	private static final String EVENTS_URL = "https://events.split.io/api/events/bulk";

	private final Configuration config;
	private final HttpClient httpClient;

	public SplitEventsClient(Configuration config) {
		this.config = config;
		this.httpClient = HttpClient.newHttpClient();
	}

	public void sendEvents(List<SplitEvent> events) {
		try {
			System.out.println("INFO - Sending batch of events: size=" + events.size());

			// Build Request
			HttpRequest request = HttpRequest.newBuilder(URI.create(EVENTS_URL))
					.header("Content-type", "application/json")
					.header("Authorization", "Bearer " + config.splitApiKey())
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
}
