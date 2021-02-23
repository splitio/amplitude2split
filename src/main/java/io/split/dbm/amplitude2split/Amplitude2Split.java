package io.split.dbm.amplitude2split;

import io.split.dbm.amplitude2split.amplitude.AmplitudeEvent;
import io.split.dbm.amplitude2split.amplitude.AmplitudeEventsClient;
import io.split.dbm.amplitude2split.split.SplitEventsClient;
import org.json.JSONObject;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Hello world!
 *
 */
public class Amplitude2Split {
	public static void main(String[] args) {
		if(args.length != 1) {
			System.err.println("ERROR - first argument should be configuration file  (e.g. java -jar amplitude2split.jar amplitude2split.config)");
			System.exit(1);
		}

		try {
			// Parse config file
			String configFilePath = args[0];
			Configuration config = Configuration.fromFile(configFilePath)
					.orElseThrow(() -> new IllegalStateException("Bad configuration file: " + configFilePath));

			System.out.println("INFO - start at " + config.jobStartTime());

			// Initialize clients
			AmplitudeEventsClient amplitudeEventsClient = new AmplitudeEventsClient(config);
			SplitEventsClient splitEventsClient = new SplitEventsClient(config);

			// Get events from Amplitude
			Stream<AmplitudeEvent> amplitudeEvents = amplitudeEventsClient.getEvents();
			Stream<JSONObject> splitEvents = amplitudeEvents
					.map(splitEventsClient::toSplitEvent)
					.flatMap(Optional::stream);

			// Send events to Split in batches
			List<JSONObject> batch = new LinkedList<>();
			splitEvents.forEach(event -> {
				batch.add(event);
				if(batch.size() >= config.batchSize) {
					splitEventsClient.sendEvents(batch);
					batch.clear();
				}
			});
			splitEventsClient.sendEvents(batch);

			// Finish
			System.out.println("INFO - Finished in " + config.jobElapsedTime() + " seconds");
			System.exit(0);
		} catch(Exception e) {
			System.err.println("ERROR - Exiting with error: " + e.getMessage());
			e.printStackTrace(System.err);
			System.exit(1);
		}
	}
}
