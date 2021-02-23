package io.split.dbm.amplitude2split.split;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import io.split.dbm.amplitude2split.Configuration;
import io.split.dbm.amplitude2split.amplitude.AmplitudeEvent;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONObject;

public class SplitEventsClient {
	private static final String EVENTS_URL = "https://events.split.io/api/events/bulk";

	private final Configuration config;
	private final CloseableHttpClient client;

	public SplitEventsClient(Configuration config) {
		this.config = config;

		this.client = HttpClients.createDefault();
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
		try {
			HttpPost httpPost = new HttpPost(EVENTS_URL);

			System.out.println("INFO - Sending batch of events: size=" + events.size());

			StringEntity entity = new StringEntity(new JSONArray(events).toString(2), StandardCharsets.UTF_8);
			httpPost.setEntity(entity);
			httpPost.setHeader("Content-type", "application/json");
			String authorizationHeader = "Bearer " + config.splitApiKey;
			httpPost.setHeader("Authorization", authorizationHeader);

			CloseableHttpResponse response = client.execute(httpPost);
			System.out.println("INFO - POST to Split status code: " + response.getStatusLine());
			if(response.getStatusLine().getStatusCode() >= 400) {
				System.err.println(response.getEntity().getContent().toString());
			}
			response.close();

			// Courtesy to minimize pressure on API
			Thread.sleep(100);
		} catch (Exception exception) {
			System.err.println("ERROR - Failed to send event");
			exception.printStackTrace(System.err);
		}
	}
}
