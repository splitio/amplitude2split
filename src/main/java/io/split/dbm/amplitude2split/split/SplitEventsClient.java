package io.split.dbm.amplitude2split.split;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Optional;

import io.split.dbm.amplitude2split.Configuration;
import io.split.dbm.amplitude2split.amplitude.AmplitudeEvent;
import org.json.JSONArray;
import org.json.JSONObject;

public class SplitEventsClient {
	private static final String EVENTS_URL = "https://events.split.io/api/events/bulk";

	private final Configuration config;
	private final HttpClient httpClient;

	public SplitEventsClient(Configuration config) {
		this.config = config;
		this.httpClient = HttpClient.newHttpClient();
	}

	public Optional<JSONObject> toSplitEvent(AmplitudeEvent event) {
		try {
			JSONObject splitEvent = new JSONObject();

			String userId = event.userId().orElseThrow(() -> new IllegalStateException("User ID is required."));
			splitEvent.put("key", userId);

			long timestamp = event.timestamp().orElseThrow(() -> new IllegalStateException("Event time is required."));
			splitEvent.put("timestamp", timestamp);

			event.value().ifPresent(val -> splitEvent.put("value", val));

			splitEvent.put("eventTypeId", event.eventTypeId());
			splitEvent.put("properties", event.properties());

			splitEvent.put("trafficTypeName", config.splitTrafficType);
			splitEvent.put("environmentName", config.splitEnvironment);

			return Optional.of(splitEvent);
		} catch (IllegalStateException exception) {
			System.err.println("WARN - Could not parse event - " + exception.getMessage());
			return Optional.empty();
		}
	}

	public void sendEvents(List<JSONObject> events) {
		System.out.println("INFO - Sending batch of events: size=" + events.size());

		try {
			HttpRequest request = HttpRequest.newBuilder(URI.create(EVENTS_URL))
					.header("Content-type", "application/json")
					.header("Authorization", "Bearer " + config.splitApiKey)
					.POST(HttpRequest.BodyPublishers.ofString(new JSONArray(events).toString()))
					.build();
			HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

			System.out.println("INFO - POST to Split status code: " + response.statusCode());
			if(response.statusCode() >= 400) {
				System.err.println(response.body());
			}

			// Courtesy to minimize pressure on API
			Thread.sleep(100);
		} catch (Exception exception) {
			System.err.println("ERROR - Failed to send event");
			exception.printStackTrace(System.err);
		}
	}
}
