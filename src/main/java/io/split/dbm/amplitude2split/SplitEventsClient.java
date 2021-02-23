package io.split.dbm.amplitude2split;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONObject;

public class SplitEventsClient {
	private static final String EVENTS_URL = "https://events.split.io/api/events/bulk";

	private final String apiToken;
	private final int batchSize;
	private final CloseableHttpClient client;

	public SplitEventsClient(String splitApiToken, int batchSize) {
		this.apiToken = splitApiToken;
		this.batchSize = batchSize;
		this.client = HttpClients.createDefault();
	}

	public void doPost(List<JSONObject> events) throws Exception {
		JSONArray batch = new JSONArray();
		for (JSONObject event : events) {
			batch.put(event);
			if (batch.length() == batchSize) {
				postToSplit(batch);
			}
		}
		postToSplit(batch);
	}

	private void postToSplit(JSONArray events) throws Exception {
		HttpPost httpPost = new HttpPost(EVENTS_URL);

		System.out.println("INFO - Sending batch of events: size=" + events.length());

		StringEntity entity = new StringEntity(events.toString(2), StandardCharsets.UTF_8);
		httpPost.setEntity(entity);
		httpPost.setHeader("Content-type", "application/json");
		String authorizationHeader = "Bearer " + apiToken;
		httpPost.setHeader("Authorization", authorizationHeader);
 
		CloseableHttpResponse response = client.execute(httpPost);
		System.out.println("INFO - POST to Split status code: " + response.getStatusLine());
		if(response.getStatusLine().getStatusCode() >= 400) {
			System.err.println(events.getJSONObject(0).toString(2));
		}
		response.close();

		// Courtesy to minimize pressure on API
		Thread.sleep(100);
	}
}
