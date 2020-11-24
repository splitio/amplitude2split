# amplitude2java

You must run with an amplitude2java.config
Sample below.

amplitude2java.config:
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
