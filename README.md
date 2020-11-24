# amplitude2java

When run, builds a start and end time that describes the past hour, then invokes Amplitudes Export API (https://developers.amplitude.com/docs/export-api) to retrieve all events during that time period.  Amplitude responds with a compressed ZIP archive.  Amplitude2Java uncompresses the archive in memory, yielding a set of GZIPd JSON files.  Amplitude2Java decompresses to JSON in memory, one file at a time, parsing the source JSON events and creating new Split JSON events.  Once a file is finished parsing, Amplitude2Java POSTs the Split Events in batches to Split's events API.  The contents of the events and the batch size is configurable.

It takes about a minute to send 40k events with the default batch size of 5000.

You must run with an amplitude2java.config

Configuration Fields:
* "amplitude.api.key" "amplitude.api.secret" Amplitude API key and secret from Amplitude UI
* "split.api.key" Split Admin API key from Split UI
* "trafficType" - Split traffic type, usually the default "user".
* "environment" - Split environment, often the default "Prod-Default".
* "eventPrefix" - Split events will prefixed with "amp." by default to make them recognizable.  Empty string is accepted.
* "key" - the Amplitude key to use for Split's event key, "user_id" by default.
* "batchSize" - the number of Split events to post to Split in each batch
* "string.property.keys" - the array of Amplitude event keys to include in Split.  You can add or remove any key.  Only root elements are found.


Sample below.

amplitude2java.config:
```
{
  "amplitude.api.key" : "094c***********dc951",
  "amplitude.api.secret" : "0fd15***********77844b3",
  "split.api.key" : "1s46c********8b321",
  "trafficType" : "user",
  "environment" : "Prod-Default",
  "eventPrefix" : "amp.",
  "key" : "user_id",
  "batchSize" : 5000,
  "string.property.keys" : [
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
