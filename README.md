# Amplitude2Split

![alt text](http://www.cortazar-split.com/Amplitude2SplitEventsIntegration.png)

To run, build the project and find the executable JAR in the target/ subdirectory.  Create an amplitude2split.config (using instructions below) and be sure to put in correct API key and secret from Amplitude and Split Admin API key.  

Run from command line (expects Java 11+):

java -jar amplitude2split-0.0.1-SNAPSHOT-jar-with-dependencies.jar amplitude2split.config

When run, builds a start and end time that describes the past hour, then invokes Amplitudes Export API (https://developers.amplitude.com/docs/export-api) to retrieve all events during that time period.  Amplitude responds with a compressed ZIP archive.  Amplitude2Split decompresses the archive in memory, yielding a set of GZIPd JSON files.  Amplitude2Split decompresses to JSON in memory, one file at a time, parsing the source JSON events and creating new Split JSON events.  Once a file finishes parsing, Amplitude2Split POSTs the Split Events in batches to Split's events API.  The contents of the events and the batch size is configurable.

It takes about a minute to send 40k events with the default batch size of 5000.

Configuration Fields:
* "amplitudeApiKey" "amplitudeApiSecret" - Amplitude API key and secret from Amplitude UI
* "splitApiKey" - Split Admin API key from Split UI
* "splitTrafficType" - Split traffic type, usually the default "user".
* "splitEnvironment" - Split environment, often the default "Prod-Default".
* "eventTypePrefix" - Split events will prefixed with "amp." by default to make them recognizable.  Empty string is accepted.
* "userIdField" - the Amplitude key to use for Split's event key, "user_id" by default.
* "valueField" - the Amplitude key to use for Split's value, empty means no value reported.  Should be a number.  Only root elements are found.
* "batchSize" - the number of Split events to post to Split in each batch
* "propertyFields" - the array of Amplitude event keys to include in Split.  You can add or remove any key.  Only root elements are found.

Sample amplitude2split.config:
```
{
  "amplitudeApiKey" : "094c***********dc951",
  "amplitudeApiSecret" : "0fd15***********77844b3",
  "splitApiKey" : "1s46c********8b321",
  "splitTrafficType" : "user",
  "splitEnvironment" : "Prod-Default",
  "eventTypePrefix" : "amp.",
  "userIdField" : "user_id",
  "valueField" : "",
  "batchSize" : 5000,
  "propertyFields" : [
  	 "country", 
  	 "language", 
  	 "device_type", 
  	 "device_carrier", 
  	 "uuid",
	 "region", 
	 "device_model", 
	 "city", 
	 "device_family", 
	 "platform", 
	 "os_version",
	 "ip_address", 
	 "os_name", 
	 "dma"
  ]
  
}
```

Sample output:

```
INFO - Starting Sync
INFO - Requesting Amplitude events: GET https://amplitude.com/api/2/export?start=20210222T21&end=20210222T22 
INFO - Processing file: 203138/203138_2021-02-22_21#627.json.gz 
INFO - Processing file: 203138/203138_2021-02-22_21#70.json.gz 
INFO - Processing file: 203138/203138_2021-02-22_22#627.json.gz 
INFO - Processing file: 203138/203138_2021-02-22_22#70.json.gz 
INFO - Sending batch of events: size=5000
INFO - Sending batch of events: size=1007
INFO - Finished sync: elapsed=6s 
```
