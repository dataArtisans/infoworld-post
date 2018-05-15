# Examples for <br> "Building Stateful Streaming Applications with Apache Flink"

This repository contains three example applications that are discussed
in the blog post *"Building Stateful Streaming Applications with Apache
Flink"*. The post presents [Apache Flink's](https://flink.apache.org)
APIs for stateful stream processing. Please have a look at a previous
blog post to learn more about
*["Stateful Stream Processing with Apache Flink"](https://www.infoworld.com/article/3267966/big-data/stateful-stream-processing-with-apache-flink.html)*.

You can fork and clone this repository to run the example applications.
All three applications process a stream of taxi ride events that is ingested
from a gzipped CSV file, which can be downloaded from Google Drive:

[Taxi Ride Data Set](https://drive.google.com/file/d/0B0TBL8JNn3JgTGNJTEJaQmFMbk0)

Modify the applications and experiment with Flink's APIs to learn more
about Flink and stateful stream processing.

**Happy Hacking!**

## Example applications

* **[WorkingTimeMontior](https://github.com/dataArtisans/infoworld-post/blob/master/src/main/java/com/dataartisans/examples/infoworld/eventdrivenapp/WorkingTimeMonitor.java)**: 
This event-driven application is based on Flink's
[DataStream API](https://ci.apache.org/projects/flink/flink-docs-stable/dev/datastream_api.html) 
and a [ProcessFunction](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/process_function.html).
The application ingests a stream of
taxi ride events and notifies taxi drivers when they have to stop
working according to work time regulations. A driver's shift may last
for at most 12 hours, meaning that rides may only be started within a
12 hour period that begins with the first ride of a shift. After a 12
hour shift, a driver needs to take a break of at least 8 hours.
The application notifies drivers until when the last ride of their
shift may be started and when they violate the regulation.

* **[AreasTotalPerHour](https://github.com/dataArtisans/infoworld-post/blob/master/src/main/java/com/dataartisans/examples/infoworld/analyticsquery/AreasTotalPerHour.java)**: 
This streaming analytics application is based on Flink's
[SQL support](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/index.html).
The application runs a SQL query on a stream of taxi ride events. The
query computes the average total amount paid per drop off-location and
hour. We discretize the coordinates of the drop-off location into cells
of 250x250 meters.

* **[AreasTotalPerHourOfDay](https://github.com/dataArtisans/infoworld-post/blob/master/src/main/java/com/dataartisans/examples/infoworld/analyticsquery/AreasTotalPerHourOfDay.java)**:
This streaming analytics application is based on Flink's
 [SQL support](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/index.html).
The application runs a SQL query on a stream of taxi ride events. The
query computes the average total amount paid per drop off-location and
hour of day. We discretize the coordinates of the drop-off location into
cells of 250x250 meters.
