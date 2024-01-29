/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

public class CalculateAverage_YourRoyalLinus_spark {
    private static final String FILE = "./1brc/measurements.txt";

    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .appName("1brc")
                .master("local[*]")
                .getOrCreate();
        StructType schema = new StructType()
                .add("station", "string")
                .add("measurement", "double");

        Dataset<Row> measurementsDF = spark.read().schema(schema).csv(FILE);
        RelationalGroupedDataset groupedMeasurements = measurementsDF.groupBy("station");

        Dataset<Row> averagedMeasurements = groupedMeasurements.avg("measurement");
        Dataset<Row> minMeasurements = groupedMeasurements.min("measurement");
        Dataset<Row> maxMeasurements = groupedMeasurements.max("measurement");

        Dataset<Row> result = averagedMeasurements.join(
                minMeasurements,
                "station").join(
                        maxMeasurements,
                        "station")
                .orderBy(
                        "station")
                .select(
                        concat(
                                col("station"), lit("="),
                                round(col("min(measurement)"), 1), lit("/"),
                                round(col("avg(measurement)"), 1), lit("/"),
                                round(col("max(measurement)"), 1)).alias("output"));

        StringBuilder output = new StringBuilder("{");
        result.collectAsList().parallelStream().forEach(
                (r) -> {
                    output.append(r.getString(0));
                    output.append(", ");
                });
        output.delete(output.length() - 2, output.length());
        output.append("}");
        System.out.println(output);
    }
}
