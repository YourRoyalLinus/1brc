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
import java.util.List;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

public class CalculateAverage_YourRoyalLinus_spark {
    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .appName("1brc")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> input = spark.read().text(FILE);

        Dataset<Row> measurementsDF = input.select(
                split(col("value"), ";").getItem(0).alias("station"),
                split(col("value"), ";").getItem(1).cast("Double").alias("measurement"));

        RelationalGroupedDataset groupedMeasurements = measurementsDF.groupBy("station");

        Dataset<Row> averagedMeasurements = groupedMeasurements.avg("measurement");
        Dataset<Row> minMeasurements = groupedMeasurements.min("measurement");
        Dataset<Row> maxMeasurements = groupedMeasurements.max("measurement");

        Dataset<Row> result = averagedMeasurements.join(
                minMeasurements, "station").join(
                        maxMeasurements, "station")
                .orderBy("station").select(
                        concat(
                                col("station"), lit("="),
                                round(col("min(measurement)"), 1), lit("/"),
                                round(col("avg(measurement)"), 1), lit("/"),
                                round(col("max(measurement)"), 1)).alias("output"));

        StringBuilder output = new StringBuilder("{");

        List<Row> results = result.collectAsList();
        for (int i = 0; i < results.size() - 1; i++) {
            output.append(results.get(i).getString(0) + ", ");
        }

        output.append(results.get(results.size() - 1).getString(0));
        output.append("}");

        System.out.println(output);
    }
}
