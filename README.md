# flink_adsb_stream
Read ADS-B stream from Kafka, parse, window, and make available via Queryable State

This is still a work in progress, but..

It expects a Kafka topic with data that looks like this:

```
{"flight": "", "timestamp_verbose": "2017-10-19 17:41:42.616481", "msg_type": "3", "track": "", "timestamp": 1508452902, "altitude": "34250", "counter": 47988, "lon": "-97.36628", "icao": "AA2E7C", "vr": "", "lat": "30.80342", "speed": ""}
{"flight": "", "timestamp_verbose": "2017-10-19 17:41:43.044733", "msg_type": "4", "track": "199", "timestamp": 1508452903, "altitude": "", "counter": 48019, "lon": "", "icao": "AA2E7C", "vr": "-1920", "lat": "", "speed": "454"}
{"flight": "", "timestamp_verbose": "2017-10-19 17:41:42.720467", "msg_type": "8", "track": "", "timestamp": 1508452902, "altitude": "", "counter": 48001, "lon": "", "icao": "AAD32F", "vr": "", "lat": "", "speed": ""}
{"flight": "", "timestamp_verbose": "2017-10-19 17:41:43.045635", "msg_type": "5", "track": "", "timestamp": 1508452903, "altitude": "10775", "counter": 48023, "lon": "", "icao": "AAD32F", "vr": "", "lat": "", "speed": ""}
{"flight": "", "timestamp_verbose": "2017-10-19 17:41:42.719107", "msg_type": "7", "track": "", "timestamp": 1508452902, "altitude": "36975", "counter": 47993, "lon": "", "icao": "0D0A16", "vr": "", "lat": "", "speed": ""}
```


