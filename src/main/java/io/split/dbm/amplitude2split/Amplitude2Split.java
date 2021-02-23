package io.split.dbm.amplitude2split;

import java.time.Duration;
import java.time.Instant;
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

			EventsHttpClient eventsClient = new EventsHttpClient(config);
			Stream<Event> events = eventsClient.getEventsFromAmplitude();
			eventsClient.sendEventsToSplitBatched(events);

			long elapsedTime = Duration.between(config.jobStart(), Instant.now()).getSeconds();
			System.out.printf("INFO - Finished sync: elapsed= %ss %n", elapsedTime);
		} catch(Exception e) {
			System.err.printf("ERROR - Exiting with error: %s %n", e.getMessage());
			e.printStackTrace(System.err);
			System.exit(1);
		}
	}
}
