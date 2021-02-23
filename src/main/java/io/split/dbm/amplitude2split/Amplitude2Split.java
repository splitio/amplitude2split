package io.split.dbm.amplitude2split;

import io.split.dbm.amplitude2split.amplitude.AmplitudeEvent;
import io.split.dbm.amplitude2split.amplitude.AmplitudeEventsClient;
import io.split.dbm.amplitude2split.split.SplitEvent;
import io.split.dbm.amplitude2split.split.SplitEventsClient;

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
			Configuration config = Configuration.fromFile(args[0]);

			System.out.println("INFO - Starting Sync");

			// Initialize clients
			AmplitudeEventsClient amplitudeEventsClient = new AmplitudeEventsClient(config);
			SplitEventsClient splitEventsClient = new SplitEventsClient(config);

			// Get events from Amplitude
			Stream<AmplitudeEvent> amplitudeEvents = amplitudeEventsClient.getEvents();
			Stream<SplitEvent> splitEvents = amplitudeEvents
					.map(SplitEvent::toSplitEvent)
					.flatMap(Optional::stream);

			// Send events to Split in batches
			List<SplitEvent> batch = new LinkedList<>();
			splitEvents.forEach(event -> {
				batch.add(event);
				if(batch.size() >= config.batchSize()) {
					splitEventsClient.sendEvents(batch);
					batch.clear();
				}
			});
			splitEventsClient.sendEvents(batch);

			// Finish
			System.out.printf("INFO - Finished sync: elapsed= %ss %n", config.jobElapsedTime());
			System.exit(0);
		} catch(Exception e) {
			System.err.printf("ERROR - Exiting with error: %s %n", e.getMessage());
			e.printStackTrace(System.err);
			System.exit(1);
		}
	}
}
