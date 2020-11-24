# amplitude2java

You must run with an amplitude2java.config

Amplitude API key and secret from Amplitude UI
Split Admin API key from Split UI
"trafficType" - Split traffic type, usually the default "user".
"environment" - Split environment, often the default "Prod-Default".
"eventPrefix" - Split events will prefixed with "amp." by default to make them recognizable.  Empty string is accepted.
"key" - the Amplitude key to use for Split's event key, "user_id" by default.
"batchSize" - the number of Split events to post to Split in each batch
"string.property.keys" - the array of Amplitude event keys to include in Split.  You can add or remove any key.  Only root elements are found.

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
