/*
 * Copyright 2018 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.examples.infoworld.eventdrivenapp;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.dataartisans.examples.infoworld.utils.TaxiRide;
import com.dataartisans.examples.infoworld.utils.TaxiRides;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * This example application ingests a stream of taxi ride events and notifies taxi drivers when
 * they have to stop working according to work time regulations.
 *
 * A driver's shift may last for at most 12 hours, meaning that rides may only be started within a
 * 12 hour period that begins with the first ride of a shift.
 * After a 12 hour shift, a driver needs to take a break of at least 8 hours.
 *
 * The application notifies drivers until when the last ride of their shift may be started and
 * when they violate the regulation.
 *
 * The stream of taxi ride events is read from a gzipped CSV file.
 * The file can be downloaded from
 *
 * https://drive.google.com/file/d/0B0TBL8JNn3JgTGNJTEJaQmFMbk0
 *
 * When starting the application the path to the file has to be provided as a parameter.
 */
public class WorkingTimeMonitor {

    public static void main(String[] args) throws Exception {

        // check parameter
        if (args.length != 1) {
            System.err.println("Please provide the path to the taxi rides file as a parameter");
        }
        String inputPath = args[0];

        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // configure event-time and watermarks
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        // ingest stream of taxi rides.
        DataStream<TaxiRide> rides = TaxiRides.getRides(env, inputPath);

        DataStream<Tuple2<String, String>> notifications = rides
            // partition stream by the driver's license id
            .keyBy(r -> r.licenseId)
            // monitor ride events and generate notifications
            .process(new MonitorWorkTime());

        // print notifications
        notifications.print();

        // run the application
        env.execute();
    }

    /**
     * A KeyedProcessFunction that monitors taxi rides for regulation compliance.
     *
     * The function emits a notification for drivers.
     */
    public static class MonitorWorkTime extends KeyedProcessFunction<String, TaxiRide, Tuple2<String, String>> {

        // time constants in milliseconds
        private static final long ALLOWED_WORK_TIME = 12 * 60 * 60 * 1000; // 12 hours
        private static final long REQ_BREAK_TIME = 8 * 60 * 60 * 1000;     // 8 hours
        private static final long CLEAN_UP_INTERVAL = 24 * 60 * 60 * 1000; // 24 hours

        private transient DateTimeFormatter formatter;

        // state handle to store the starting time of a shift
        ValueState<Long> shiftStart;

        @Override
        public void open(Configuration conf) {
            // register state handle
            shiftStart = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("shiftStart", Types.LONG));
            // initialize time formatter
            this.formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        }

        @Override
        public void processElement(
                TaxiRide ride,
                Context ctx,
                Collector<Tuple2<String, String>> out) throws Exception {

            // look up start time of the last shift
            Long startTs = shiftStart.value();

            if (startTs == null ||
                    startTs < ride.pickUpTime - (ALLOWED_WORK_TIME + REQ_BREAK_TIME)) {
                // this is the first ride of a new shift.
                startTs = ride.pickUpTime;
                shiftStart.update(startTs);
                long endTs = startTs + ALLOWED_WORK_TIME;
                out.collect(Tuple2.of(ride.licenseId,
                    "New shift started. Shift ends at " + formatter.print(endTs) + "."));
                // register timer to clean up the state in 24h
                ctx.timerService().registerEventTimeTimer(startTs + CLEAN_UP_INTERVAL);
            } else if (startTs < ride.pickUpTime - ALLOWED_WORK_TIME) {
                // this ride started after the allowed work time ended.
                // it is a violation of the regulations!
                out.collect(Tuple2.of(ride.licenseId,
                    "This ride violated the working time regulations."));
            }
        }

        @Override
        public void onTimer(
                long timerTs,
                OnTimerContext ctx,
                Collector<Tuple2<String, String>> out) throws Exception {

            // remove the shift state if no new shift was started already.
            Long startTs = shiftStart.value();
            if (startTs == timerTs - CLEAN_UP_INTERVAL) {
                shiftStart.clear();
            }
        }
    }
}
