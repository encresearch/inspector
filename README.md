# INSPECTOR - Earthquake Precursors Anomaly Detection Framework

Python application that constantly scans data and detects breaking of thresholds values and sends an alert as an email.

This service is part of our [Earthquake Data Assimilation System](https://github.com/encresearch/data-assimilation-system).

## API Overview
`Inspector` interprets data and initiates an alert by sending `Notifier` a JSON object containing the anomaly detected in the data. More information on `Notifier` can be found here: https://github.com/encresearch/notifier

`Inspector` connects to our MQTT Broker, and publishes data on anomalies that it detects for `Notifier` to relay to our subscribers. To detect these anomalies, each type of data has its own designated `examineData` Generator Object. This Coroutine runs in realtime for each data stream from each location, and detects the anomalies forming and ending. At both the beginning and end of a data anomaly, inspector publishes a JSON object in the following format to our MQTT broker.

~~~
{
  'topic': ____,
  'anomaly_status', ____,
  'location': ____,
  'time_init: ____',
  'time_duration': ____
}
~~~

Added flexability to the JSON Object structure, object-oriented style, and more data on anomalies will be future features in `Inspector` and `Notifier`

## Getting Started
These instructions are to get notifier up and running in your local development environment.

### Install and Run Locally

Pending.

**Run Locally**

Pending.

**Run Local Tests**

Pending.


## Contributing
Pull requests and stars are always welcome. To contribute, please fetch, create an issue explaining the bug or feature request, create a branch off this issue and submit a pull request.
