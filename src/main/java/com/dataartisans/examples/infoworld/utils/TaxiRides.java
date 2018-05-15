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
package com.dataartisans.examples.infoworld.utils;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.types.Row;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Utility class to ingest a stream of taxi ride events.
 */
public class TaxiRides {

    // field types of the CSV file
    private static TypeInformation[] inputFieldTypes = new TypeInformation[]{
        Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.FLOAT,
        Types.FLOAT, Types.FLOAT, Types.FLOAT, Types.FLOAT,
        Types.STRING, Types.FLOAT, Types.FLOAT, Types.FLOAT, Types.FLOAT, Types.FLOAT, Types.FLOAT
    };

    /**
     * Returns a DataStream of TaxiRide events from a CSV file.
     *
     * @param env The execution environment.
     * @param csvFile The path of the CSV file to read.
     * @return A DataStream of TaxiRide events.
     */
    public static DataStream<TaxiRide> getRides(StreamExecutionEnvironment env, String csvFile) {

        // create input format to read the CSV file
        RowCsvInputFormat inputFormat = new RowCsvInputFormat(
                null, // input path is configured later
                inputFieldTypes,
                "\n",
                ",");

        // read file sequentially (with a parallelism of 1)
        DataStream<Row> parsedRows = env
                .readFile(inputFormat, csvFile)
                .returns(Types.ROW(inputFieldTypes))
                .setParallelism(1);

        // convert parsed CSV rows into TaxiRides, extract timestamps, and assign watermarks
        return parsedRows
                // map to TaxiRide POJOs
                .map(new RideMapper())
                // define drop-off time as event-time timestamps and generate ascending watermarks.
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TaxiRide>() {
                    @Override
                    public long extractAscendingTimestamp(TaxiRide ride) {
                        return ride.dropOffTime;
                    }
                });
    }

    /**
     * MapFunction to generate TaxiRide POJOs from parsed CSV data.
     */
    public static class RideMapper extends RichMapFunction<Row, TaxiRide> {
        private transient DateTimeFormatter formatter;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        }

        @Override
        public TaxiRide map(Row row) throws Exception {
            // convert time strings into timestamps (longs)
            long pickupTime = formatter.parseDateTime((String) row.getField(2)).getMillis();
            long dropoffTime = formatter.parseDateTime((String) row.getField(3)).getMillis();

            // create POJO and set all fields
            TaxiRide ride = new TaxiRide();
            ride.medallion = (String) row.getField(0);
            ride.licenseId = (String) row.getField(1);
            ride.pickUpTime = pickupTime;
            ride.dropOffTime = dropoffTime;
            ride.pickUpLon = (float) row.getField(6);
            ride.pickUpLat = (float) row.getField(7);
            ride.dropOffLon = (float) row.getField(8);
            ride.dropOffLat = (float) row.getField(9);
            ride.total = (float) row.getField(16);
            return ride;
        }
    }
}
