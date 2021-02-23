package io.split.dbm.amplitude2split;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONObject;

/**
 * Hello world!
 *
 */
public class Amplitude2Split {
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd'T'HH");
	private static final SimpleDateFormat SERVER_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private static final SimpleDateFormat SERVER_FORMAT_2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static void main( String[] args) {
		if(args.length != 1) {
			System.err.println("ERROR - first argument should be configuration file  (e.g. java -jar amplitude2split.jar amplitude2split.config)");
			System.exit(1);
		}

		try {
			String configFilePath = args[0];
			Amplitude2Split.execute(configFilePath);
			System.exit(0);
		} catch(Exception e) {
			System.err.println("ERROR - exiting with error: " + e.getMessage());
			e.printStackTrace(System.err);
			System.exit(1);
		}
	}

	private static void execute(String configFilePath) throws Exception {
		Configuration config = Configuration.fromFile(configFilePath)
				.orElseThrow(() -> new IllegalStateException("Bad configuration file: " + configFilePath));

		SplitEventsClient creator = new SplitEventsClient(config.SPLIT_API_TOKEN, config.BATCH_SIZE);

		HttpResponse response = getEventsFromAmplitude(config);
		int statusCode = response.getStatusLine().getStatusCode();
		System.out.println("INFO - status code: " + statusCode);

		if(statusCode >= 200 && statusCode < 300) {
			ZipArchiveInputStream zis = parseEventFiles(response);
			ZipArchiveEntry entry;
			while ((entry = zis.getNextZipEntry()) != null) {
				List<JSONObject> events = parseEventsFromFile(zis, entry, config)
						.collect(Collectors.toList());
				creator.doPost(events);
				System.out.println("INFO - processed " + events.size() + " events");
			}
		} else {
			System.err.println("WARN - exiting with error on data extraction API call...");
		}
		System.out.println("INFO - finish in " + ((System.currentTimeMillis() - config.jobStart) / 1000) + "s");
	}

	private static HttpResponse getEventsFromAmplitude(Configuration config) throws IOException {
		Instant nowUtc = Instant.now();
		Instant hourAgoUtc = nowUtc.minus(1, ChronoUnit.HOURS);
		Date now = Date.from(nowUtc);
		Date hourAgo = Date.from(hourAgoUtc);

		System.out.println("INFO - start at " + SERVER_FORMAT_2.format(new Date(config.jobStart)));

		String end = DATE_FORMAT.format(now);
		String start = DATE_FORMAT.format(hourAgo);

		CredentialsProvider provider = new BasicCredentialsProvider();
		UsernamePasswordCredentials credentials
				= new UsernamePasswordCredentials(config.AMPLITUDE_API_TOKEN, config.AMPLITUDE_API_SECRET);
		provider.setCredentials(AuthScope.ANY, credentials);

		HttpClient client = HttpClientBuilder.create()
				.setDefaultCredentialsProvider(provider)
				.build();

		String uri = "https://amplitude.com/api/2/export?start=" + start + "&end=" + end;
		System.out.println("INFO - GET " + uri);

		return client.execute(new HttpGet(uri));
	}

	private static ZipArchiveInputStream parseEventFiles(HttpResponse response) throws IOException {
		// Handle the wrapper ZIP
		BufferedInputStream bis = new BufferedInputStream(response.getEntity().getContent());
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		IOUtils.copy(bis, baos);

		return new ZipArchiveInputStream(
				new BufferedInputStream(new ByteArrayInputStream(baos.toByteArray())),
				"UTF-8", false, true);
	}

	private static Stream<JSONObject> parseEventsFromFile(ZipArchiveInputStream zis, ZipArchiveEntry entry, Configuration config) throws IOException {
		// Handle the Gzipd JSON
		System.out.println("INFO - " + entry.getName());

		// Load GZipped file from Archive
		ByteArrayOutputStream zipBaos = new ByteArrayOutputStream();
		IOUtils.copy(zis, zipBaos);
		GZIPInputStream gzis = new GZIPInputStream(new ByteArrayInputStream(zipBaos.toByteArray()));
		ByteArrayOutputStream jsonBaos = new ByteArrayOutputStream();
		IOUtils.copy(gzis, jsonBaos);

		// Read Events From File
		BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(jsonBaos.toByteArray())));
		return reader.lines()
				.map(line -> createEvent(line, config))
				.flatMap(Optional::stream);
	}

	private static Optional<JSONObject> createEvent(String line, Configuration config) {
		try {
			JSONObject amplitudeEvent = new JSONObject(line);

			JSONObject splitEvent = new JSONObject();
			splitEvent.put("trafficTypeName", config.SPLIT_TRAFFIC_TYPE);
			splitEvent.put("environmentTypeName", config.SPLIT_ENVIRONMENT);

			String user_id = getUserId(amplitudeEvent, config.USER_ID)
					.orElseThrow(() -> new IllegalStateException("User ID is required."));
			splitEvent.put("key", user_id);

			// no value if there's an empty or missing key
			if(amplitudeEvent.has(config.VALUE_KEY)) {
				Object valueObj = amplitudeEvent.get(config.VALUE_KEY);
				if(valueObj instanceof Integer || valueObj instanceof Float) {
					splitEvent.put("value", valueObj);
				} else {
					System.err.println("WARN - value was neither integer or float: " + valueObj);
				}
			}

			String eventTypeId = config.AMPLITUDE_PREFIX + "null";
			if(amplitudeEvent.has("event_type")) {
				eventTypeId = config.AMPLITUDE_PREFIX + amplitudeEvent.getString("event_type");
			}
			splitEvent.put("eventTypeId", eventTypeId);

			String serverUploadTime = amplitudeEvent.getString("event_time");
			Date parsedServerTime;
			try {
				parsedServerTime = SERVER_FORMAT.parse(serverUploadTime);
			} catch (ParseException pe) {
				try {
					parsedServerTime = SERVER_FORMAT_2.parse(serverUploadTime);
				} catch (ParseException e) {
					System.err.println("ERROR - event_time could not be parsed");
					return Optional.empty();
				}
			}
			splitEvent.put("timestamp", parsedServerTime.getTime());

			JSONObject propertiesObj = new JSONObject();

			for(String propertyKey : config.STRING_PROPERTY_KEYS) {
				addStringProperty(amplitudeEvent, propertiesObj, "", propertyKey);
			}

			if(amplitudeEvent.has("user_properties")) {
				JSONObject userPropsObj = amplitudeEvent.getJSONObject("user_properties");
				String[] userPropsKeys
						= new String[] { "organizationPlatType", "role", "organizationName",
						"organizationSupportPlanType"};
				for(String userPropKey : userPropsKeys) {
					addStringProperty(userPropsObj, propertiesObj, "user_properties.", userPropKey);
				}
			}

			splitEvent.put("properties", propertiesObj);

			return Optional.of(splitEvent);
		} catch (IllegalStateException exception) {
			System.err.println("WARN - Could not parse event - " + exception.getMessage());
			return Optional.empty();
		}
	}

	private static Optional<String> getUserId(JSONObject amplitudeEvent, String userIdField) {
		String userId = "";
		if(amplitudeEvent.has(userIdField)) {
			Object userObj = amplitudeEvent.get(userIdField);
			if(userObj instanceof String) {
				userId = amplitudeEvent.getString(userIdField);
			} else {
				System.err.println("WARN - " + userIdField + " is not a string: " + userObj.toString());
				userId = userObj.toString();
			}
		}
		if(userId.isEmpty()) {
			System.err.println("WARN - " + userIdField + " not found for event: " + amplitudeEvent.toString(2));
			return Optional.empty();
		}
		return Optional.of(userId);
	}

	private static void addStringProperty(JSONObject src, JSONObject dest, String prefix, String name) {
		String value = "";
		if(src.has(name) && !src.isNull(name)) {
			value = src.getString(name);
		}
		dest.put(prefix + name, value);
	}
}
