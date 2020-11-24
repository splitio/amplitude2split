# amplitude2java

![alt text](http://www.cortazar-split.com/Amplitude2SplitEventsIntegration.png)

To run, build the project and find the executable JAR in the target/ subdirectory.  Create an amplitude2java.config (using instructions below) and be sure to put in correct API key and secret from Amplitude and Split Admin API key.  

Run from command line (expects Java 8+):

java -jar amplitude2java-0.0.1-SNAPSHOT-jar-with-dependencies.jar amplitude2java.config

When run, builds a start and end time that describes the past hour, then invokes Amplitudes Export API (https://developers.amplitude.com/docs/export-api) to retrieve all events during that time period.  Amplitude responds with a compressed ZIP archive.  Amplitude2Java uncompresses the archive in memory, yielding a set of GZIPd JSON files.  Amplitude2Java decompresses to JSON in memory, one file at a time, parsing the source JSON events and creating new Split JSON events.  Once a file is finished parsing, Amplitude2Java POSTs the Split Events in batches to Split's events API.  The contents of the events and the batch size is configurable.

It takes about a minute to send 40k events with the default batch size of 5000.

Configuration Fields:
* "amplitude.api.key" "amplitude.api.secret" - Amplitude API key and secret from Amplitude UI
* "split.api.key" - Split Admin API key from Split UI
* "trafficType" - Split traffic type, usually the default "user".
* "environment" - Split environment, often the default "Prod-Default".
* "eventPrefix" - Split events will prefixed with "amp." by default to make them recognizable.  Empty string is accepted.
* "key" - the Amplitude key to use for Split's event key, "user_id" by default.
* "value" - the Amplitude key to use for Split's value, empty means no value reported.  Should be an integer or float.  Only root elements are found.
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
  "value" : "",
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

Sample output below...

```
INFO - start at 2020-11-24 10:57:36
INFO - GET https://amplitude.com/api/2/export?start=20201124T09&end=20201124T10
INFO - status code: 200
INFO - 203138/203138_2020-11-24_9#627.json.gz
INFO - sending events 0 ->  5000 of 7381
INFO - POST to Split status code: HTTP/1.1 202 Accepted
INFO - sending events 5000 ->  7381 of 7381
INFO - POST to Split status code: HTTP/1.1 202 Accepted
INFO - processed 7381 events
INFO - last input event: {
  "country": "Canada",
  "data": {
    "group_ids": {"3124": [2357687219]},
    "group_first_event": {}
  },
  "$schema": 12,
  "language": "English",
  "device_type": null,
  "device_carrier": null,
  "uuid": "d0b5b3a6-2e3b-11eb-9a80-0a62134d514d",
  "user_creation_time": "2020-07-09 18:51:29.664000",
  "location_lng": null,
  "event_type": "segment.segment.definition.permission_paywall.load",
  "paying": null,
  "location_lat": null,
  "app": 203138,
  "device_id": "a1c9357b-b104-41c5-b4be-2b411489123d",
  "group_properties": {"[Segment] Group": {"90f978f0-5e4e-11ea-9bb3-0ed25467b33f": {}}},
  "idfa": null,
  "server_upload_time": "2020-11-24 09:59:58.880000",
  "amplitude_attribution_ids": null,
  "server_received_time": "2020-11-24 09:59:58.863000",
  "user_id": "1fa23f90-c215-11ea-a639-02ed1811fa3b",
  "region": "Newfoundland and Labrador",
  "amplitude_event_type": null,
  "processed_time": "2020-11-24 10:00:00.480098",
  "device_model": "Mac OS",
  "user_properties": {
    "organizationId": "90f978f0-5e4e-11ea-9bb3-0ed25467b33f",
    "organizationPlanType": "FREE",
    "role": "Member",
    "organizationName": "Mysa Smart Thermostats",
    "organizationSupportPlanType": "BASIC",
    "name": "steve",
    "id": "1fa23f90-c215-11ea-a639-02ed1811fa3b",
    "[Segment] Group": "90f978f0-5e4e-11ea-9bb3-0ed25467b33f",
    "email": "steve@getmysa.com"
  },
  "city": "St. John's",
  "start_version": null,
  "is_attribution_event": false,
  "client_event_time": "2020-11-24 09:59:55.977000",
  "device_family": "Mac OS",
  "platform": "Web",
  "device_manufacturer": null,
  "library": "segment",
  "adid": null,
  "$insert_id": "ajs-de09c0da8f0c71f2e67cdae5a440e610:CS4fUc2U9U",
  "client_upload_time": "2020-11-24 09:59:58.863000",
  "event_properties": {
    "eventTypeId": "segment.segment.definition.permission_paywall.load",
    "split": [{"account": "90f978f0-5e4e-11ea-9bb3-0ed25467b33f"}]
  },
  "amplitude_id": 176632796945,
  "os_version": "86",
  "session_id": -1,
  "groups": {"[Segment] Group": ["90f978f0-5e4e-11ea-9bb3-0ed25467b33f"]},
  "ip_address": "156.57.42.34",
  "event_id": 213979667,
  "version_name": null,
  "sample_rate": null,
  "device_brand": null,
  "os_name": "Chrome",
  "dma": null,
  "event_time": "2020-11-24 09:59:55.977000"
}
INFO - last split event: {
  "eventTypeId": "amp.segment.segment.definition.permission_paywall.load",
  "environmentTypeName": "Prod-Default",
  "trafficTypeName": "user",
  "value": 213979667,
  "key": "1fa23f90-c215-11ea-a639-02ed1811fa3b",
  "properties": {
    "country": "Canada",
    "user_properties.role": "Member",
    "device_model": "Mac OS",
    "user_properties.organizationSupportPlanType": "BASIC",
    "city": "St. John's",
    "os_version": "86",
    "device_family": "Mac OS",
    "language": "English",
    "device_type": "",
    "device_carrier": "",
    "ip_address": "156.57.42.34",
    "uuid": "d0b5b3a6-2e3b-11eb-9a80-0a62134d514d",
    "platform": "Web",
    "user_properties.organizationName": "Mysa Smart Thermostats",
    "user_properties.organizationPlatType": "",
    "os_name": "Chrome",
    "dma": "",
    "region": "Newfoundland and Labrador"
  },
  "timestamp": 1606234572000
}
INFO - 203138/203138_2020-11-24_9#70.json.gz
INFO - sending events 0 ->  5000 of 9852
INFO - POST to Split status code: HTTP/1.1 202 Accepted
INFO - sending events 5000 ->  9852 of 9852
INFO - POST to Split status code: HTTP/1.1 202 Accepted
INFO - processed 9852 events
INFO - last input event: {
  "country": "Singapore",
  "data": {
    "group_ids": {"3124": [1278071936]},
    "group_first_event": {}
  },
  "$schema": 12,
  "language": "English",
  "device_type": null,
  "device_carrier": null,
  "uuid": "d0ff9eb2-2e3b-11eb-ab67-06153b785aa1",
  "user_creation_time": "2019-09-30 05:56:31.996000",
  "location_lng": null,
  "event_type": "segment.split.definition.permission_paywall.load",
  "paying": null,
  "location_lat": null,
  "app": 203138,
  "device_id": "eb953a39-5a19-474f-9b45-a8f920792216",
  "group_properties": {"[Segment] Group": {"e6a3e0f0-acf7-11e9-ae79-0a1c02136776": {}}},
  "idfa": null,
  "server_upload_time": "2020-11-24 09:59:59.790000",
  "amplitude_attribution_ids": null,
  "server_received_time": "2020-11-24 09:59:59.782000",
  "user_id": "f6397440-d8ed-11e9-8ea5-0af1e247df84",
  "amplitude_event_type": null,
  "region": null,
  "processed_time": "2020-11-24 10:00:00.949083",
  "device_model": "Mac OS",
  "user_properties": {
    "organizationId": "e6a3e0f0-acf7-11e9-ae79-0a1c02136776",
    "role": "Admin",
    "organizationPlanType": "FREE",
    "organizationName": "Sephora",
    "organizationSupportPlanType": "BASIC",
    "name": "it",
    "id": "f6397440-d8ed-11e9-8ea5-0af1e247df84",
    "[Segment] Group": "e6a3e0f0-acf7-11e9-ae79-0a1c02136776",
    "email": "it@sephoradigital.com"
  },
  "city": "Singapore",
  "start_version": null,
  "is_attribution_event": false,
  "device_family": "Mac OS",
  "client_event_time": "2020-11-24 09:59:57.447000",
  "platform": "Web",
  "device_manufacturer": null,
  "library": "segment",
  "adid": null,
  "$insert_id": "ajs-02db4c85d4d8572961fc9949c386960f:CS4fUc2U9U",
  "client_upload_time": "2020-11-24 09:59:59.782000",
  "event_properties": {
    "eventTypeId": "segment.split.definition.permission_paywall.load",
    "split": [{"account": "e6a3e0f0-acf7-11e9-ae79-0a1c02136776"}]
  },
  "amplitude_id": 111810723906,
  "os_version": "86",
  "session_id": -1,
  "groups": {"[Segment] Group": ["e6a3e0f0-acf7-11e9-ae79-0a1c02136776"]},
  "ip_address": "54.255.85.169",
  "version_name": null,
  "event_id": 500480304,
  "sample_rate": null,
  "device_brand": null,
  "os_name": "Chrome",
  "dma": null,
  "event_time": "2020-11-24 09:59:57.447000"
}
INFO - last split event: {
  "eventTypeId": "amp.segment.split.definition.permission_paywall.load",
  "environmentTypeName": "Prod-Default",
  "trafficTypeName": "user",
  "value": 500480304,
  "key": "f6397440-d8ed-11e9-8ea5-0af1e247df84",
  "properties": {
    "country": "Singapore",
    "user_properties.role": "Admin",
    "device_model": "Mac OS",
    "user_properties.organizationSupportPlanType": "BASIC",
    "city": "Singapore",
    "os_version": "86",
    "device_family": "Mac OS",
    "language": "English",
    "device_type": "",
    "device_carrier": "",
    "ip_address": "54.255.85.169",
    "uuid": "d0ff9eb2-2e3b-11eb-ab67-06153b785aa1",
    "platform": "Web",
    "user_properties.organizationName": "Sephora",
    "user_properties.organizationPlatType": "",
    "os_name": "Chrome",
    "dma": "",
    "region": ""
  },
  "timestamp": 1606234044000
}
INFO - 203138/203138_2020-11-24_10#627.json.gz
INFO - sending events 0 ->  5000 of 7531
INFO - POST to Split status code: HTTP/1.1 202 Accepted
INFO - sending events 5000 ->  7531 of 7531
INFO - POST to Split status code: HTTP/1.1 202 Accepted
INFO - processed 7531 events
INFO - last input event: {
  "country": "United States",
  "$schema": 12,
  "data": {
    "group_ids": {"3124": [2331954678]},
    "group_first_event": {}
  },
  "device_type": "Windows",
  "language": "English",
  "device_carrier": null,
  "uuid": "33419794-2e44-11eb-959d-0a1cc06edb13",
  "user_creation_time": "2020-09-08 17:02:32.344000",
  "location_lng": null,
  "event_type": "segment.admin.users.paywall.load",
  "paying": null,
  "location_lat": null,
  "app": 203138,
  "device_id": "a27908c8-2c4b-46f6-8a13-8202ef6f965c",
  "group_properties": {"[Segment] Group": {"e04e2790-599c-11ea-8dff-0ed25467b33f": {}}},
  "idfa": null,
  "server_upload_time": "2020-11-24 10:59:58.521000",
  "amplitude_attribution_ids": null,
  "server_received_time": "2020-11-24 10:59:58.505000",
  "user_id": "121be140-f1f5-11ea-9448-0eefc8c718cd",
  "processed_time": "2020-11-24 11:00:01.789159",
  "region": "Michigan",
  "amplitude_event_type": null,
  "device_model": "Windows",
  "city": "Grand Blanc",
  "user_properties": {
    "organizationId": "e04e2790-599c-11ea-8dff-0ed25467b33f",
    "organizationPlanType": "PAID",
    "role": "Admin",
    "organizationName": "Quicken Loans",
    "organizationSupportPlanType": "STANDARD",
    "name": "KP",
    "id": "121be140-f1f5-11ea-9448-0eefc8c718cd",
    "[Segment] Group": "e04e2790-599c-11ea-8dff-0ed25467b33f",
    "email": "kevinprice@quickenloans.com"
  },
  "start_version": null,
  "is_attribution_event": false,
  "device_family": "Windows",
  "client_event_time": "2020-11-24 10:59:55.754000",
  "platform": "Web",
  "device_manufacturer": null,
  "library": "segment",
  "adid": null,
  "$insert_id": "ajs-6aa9256803493f74b617c2172b3ccb20:CS4fUc2U9U",
  "client_upload_time": "2020-11-24 10:59:58.505000",
  "event_properties": {
    "eventTypeId": "segment.admin.users.paywall.load",
    "split": [{"account": "e04e2790-599c-11ea-8dff-0ed25467b33f"}]
  },
  "amplitude_id": 190636399399,
  "os_version": "86",
  "groups": {"[Segment] Group": ["e04e2790-599c-11ea-8dff-0ed25467b33f"]},
  "session_id": -1,
  "ip_address": "68.62.33.105",
  "version_name": null,
  "event_id": 923914363,
  "sample_rate": null,
  "device_brand": null,
  "dma": "Flint-Saginaw-Bay City, MI",
  "os_name": "Chrome",
  "event_time": "2020-11-24 10:59:55.754000"
}
INFO - last split event: {
  "eventTypeId": "amp.segment.admin.users.paywall.load",
  "environmentTypeName": "Prod-Default",
  "trafficTypeName": "user",
  "value": 923914363,
  "key": "121be140-f1f5-11ea-9448-0eefc8c718cd",
  "properties": {
    "country": "United States",
    "user_properties.role": "Admin",
    "device_model": "Windows",
    "user_properties.organizationSupportPlanType": "STANDARD",
    "city": "Grand Blanc",
    "os_version": "86",
    "device_family": "Windows",
    "language": "English",
    "device_type": "Windows",
    "device_carrier": "",
    "ip_address": "68.62.33.105",
    "uuid": "33419794-2e44-11eb-959d-0a1cc06edb13",
    "platform": "Web",
    "user_properties.organizationName": "Quicken Loans",
    "user_properties.organizationPlatType": "",
    "os_name": "Chrome",
    "dma": "Flint-Saginaw-Bay City, MI",
    "region": "Michigan"
  },
  "timestamp": 1606237949000
}
INFO - 203138/203138_2020-11-24_10#70.json.gz
INFO - sending events 0 ->  5000 of 8891
INFO - POST to Split status code: HTTP/1.1 202 Accepted
INFO - sending events 5000 ->  8891 of 8891
INFO - POST to Split status code: HTTP/1.1 202 Accepted
INFO - processed 8891 events
INFO - last input event: {
  "country": "Denmark",
  "$schema": 12,
  "data": {
    "group_ids": {"3124": [660382809]},
    "group_first_event": {}
  },
  "device_type": "Windows",
  "language": "Danish",
  "device_carrier": null,
  "uuid": "340be396-2e44-11eb-b57e-064f0af10148",
  "user_creation_time": "2019-03-28 09:38:49.264000",
  "location_lng": null,
  "event_type": "segment.nav.splits.click",
  "paying": null,
  "location_lat": null,
  "app": 203138,
  "device_id": "0117075b-6d92-4721-beb9-d253960e30aa",
  "group_properties": {"[Segment] Group": {"e2c82960-294d-11e9-977e-0a3ac22449da": {}}},
  "idfa": null,
  "server_upload_time": "2020-11-24 10:59:59.235000",
  "amplitude_attribution_ids": null,
  "server_received_time": "2020-11-24 10:59:59.224000",
  "user_id": "408a6f60-513d-11e9-9f5e-1268881cf0a2",
  "processed_time": "2020-11-24 11:00:03.147454",
  "region": "Central Jutland",
  "amplitude_event_type": null,
  "device_model": "Windows",
  "user_properties": {
    "organizationId": "e2c82960-294d-11e9-977e-0a3ac22449da",
    "organizationPlanType": "PAID",
    "role": "Admin",
    "organizationName": "Coop",
    "organizationSupportPlanType": "STANDARD",
    "name": "asger.fallesen",
    "id": "408a6f60-513d-11e9-9f5e-1268881cf0a2",
    "[Segment] Group": "e2c82960-294d-11e9-977e-0a3ac22449da",
    "email": "asger.fallesen@coop.dk"
  },
  "start_version": null,
  "city": "Aarhus C",
  "is_attribution_event": false,
  "client_event_time": "2020-11-24 10:59:56.725000",
  "device_family": "Windows",
  "platform": "Web",
  "device_manufacturer": null,
  "library": "segment",
  "adid": null,
  "$insert_id": "ajs-3f9d801405828be8ac7c1102260eed15:CS4fUc2U9U",
  "client_upload_time": "2020-11-24 10:59:59.224000",
  "event_properties": {
    "eventTypeId": "segment.nav.splits.click",
    "split": [{"account": "e2c82960-294d-11e9-977e-0a3ac22449da"}]
  },
  "amplitude_id": 89079084766,
  "os_version": "86",
  "groups": {"[Segment] Group": ["e2c82960-294d-11e9-977e-0a3ac22449da"]},
  "session_id": -1,
  "ip_address": "212.237.135.167",
  "event_id": 922344899,
  "version_name": null,
  "sample_rate": null,
  "device_brand": null,
  "dma": null,
  "os_name": "Chrome",
  "event_time": "2020-11-24 10:59:56.725000"
}
INFO - last split event: {
  "eventTypeId": "amp.segment.nav.splits.click",
  "environmentTypeName": "Prod-Default",
  "trafficTypeName": "user",
  "value": 922344899,
  "key": "408a6f60-513d-11e9-9f5e-1268881cf0a2",
  "properties": {
    "country": "Denmark",
    "user_properties.role": "Admin",
    "device_model": "Windows",
    "user_properties.organizationSupportPlanType": "STANDARD",
    "city": "Aarhus C",
    "os_version": "86",
    "device_family": "Windows",
    "language": "Danish",
    "device_type": "Windows",
    "device_carrier": "",
    "ip_address": "212.237.135.167",
    "uuid": "340be396-2e44-11eb-b57e-064f0af10148",
    "platform": "Web",
    "user_properties.organizationName": "Coop",
    "user_properties.organizationPlatType": "",
    "os_name": "Chrome",
    "dma": "",
    "region": "Central Jutland"
  },
  "timestamp": 1606237921000
}
INFO - finish in 40s
```
