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

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.stream.Collector;

import static java.util.stream.Collectors.groupingBy;

public class CalculateAverage_YourRoyalLinus {

    private static final String FILE = "./measurements2.txt";

    private static final int BUFFER_SIZE = 10000;

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private static record ResultRow(double min, double mean, double max) {

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    private static class Aggregator implements Runnable {
        private final List<String> input;

        private Map<String, MeasurementAggregator> aggregatedResult;

        Aggregator(String[] lines) {
            input = Arrays.asList(lines);
        }

        @Override
        public void run() {
            Collector<Measurement, MeasurementAggregator, MeasurementAggregator> collector = Collector.of(
                    MeasurementAggregator::new,
                    (a, m) -> {
                        a.min = Math.min(a.min, m.value);
                        a.max = Math.max(a.max, m.value);
                        a.sum += m.value;
                        a.count++;
                    },
                    (agg1, agg2) -> {
                        var res = new MeasurementAggregator();
                        res.min = Math.min(agg1.min, agg2.min);
                        res.max = Math.max(agg1.max, agg2.max);
                        res.sum = agg1.sum + agg2.sum;
                        res.count = agg1.count + agg2.count;

                        return res;
                    },
                    agg -> {
                        var res = new MeasurementAggregator();
                        res.min = agg.min;
                        res.max = agg.max;
                        res.sum = agg.sum;
                        res.count = agg.count;

                        return res;
                    });

            aggregatedResult = input.stream()
                    .map(l -> new Measurement(l.split(";")))
                    .collect(groupingBy(Measurement::station, collector));
        }

        public Map<String, MeasurementAggregator> getResult() {
            return aggregatedResult;
        }
    }

    private static String[] getNextBatch(Scanner scanner) {
        String[] lines = new String[BUFFER_SIZE];
        int i = 0;
        while (scanner.hasNextLine() && i < BUFFER_SIZE) {
            lines[i++] = scanner.nextLine();
        }

        return lines;
    }

    public static void main(String[] args) throws IOException {
        try (var scanner = new Scanner(new File(FILE))) {
            int numThreads = Runtime.getRuntime().availableProcessors();
            List<Aggregator> aggregators = new ArrayList<>();
            try (var execService = Executors.newFixedThreadPool(numThreads)) {
                while (scanner.hasNextLine()) {
                    String[] buf = getNextBatch(scanner);
                    Aggregator a = new Aggregator(buf);
                    aggregators.add(a);
                    execService.execute(a);
                }

                Map<String, MeasurementAggregator> measurements = aggregators
                        .stream()
                        .map(Aggregator::getResult)
                        .reduce(new HashMap<>(), (a, b) -> {
                            a.keySet().forEach((k) -> {
                                MeasurementAggregator _child = a.get(k);
                                MeasurementAggregator _parent = b.getOrDefault(k, null);
                                if (_parent == null) {
                                    b.put(k, _child);
                                }
                                else {
                                    MeasurementAggregator _aggr = new MeasurementAggregator();
                                    _aggr.min = Math.min(_parent.min, _child.min);
                                    _aggr.max = Math.max(_parent.max, _child.max);
                                    _aggr.sum = _parent.sum + _child.sum;
                                    _aggr.count = _parent.count + _child.count;

                                    b.put(k, _aggr);
                                }
                            });
                            return b;
                        });
                SortedMap<String, ResultRow> output = new TreeMap<>();
                measurements.forEach((k, v) -> {
                    ResultRow r = new ResultRow(v.min, (Math.round(v.sum * 10.0) / 10.0) / v.count, v.max);
                    output.put(k, r);
                });

                System.out.println(output);
            }
        }
        catch (Exception ex) {
            System.out.println("Exception:\n" + ex);
            System.exit(1);
        }
    }
}

// new MeasurementAggregator(agg.min, (Math.round(agg.sum * 10.0) / 10.0) / agg.count, agg.max)