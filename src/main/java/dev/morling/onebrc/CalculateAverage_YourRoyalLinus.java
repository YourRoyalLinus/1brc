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
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public class CalculateAverage_YourRoyalLinus {
    private static final String FILE = "./measurements.txt";
    private static final ConcurrentHashMap<String, ResultRecord> concurrentMap = new ConcurrentHashMap<>();

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
        File file = new File(FILE);
        long start = System.currentTimeMillis();

        getFilePartitions(file).stream().parallel().forEach(partition -> {
            long partitionEnd = partition.end();
            try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(Path.of(FILE), StandardOpenOption.READ)) {
                MappedByteBuffer byteBuf = fileChannel.map(FileChannel.MapMode.READ_ONLY, partition.start(), partitionEnd - partition.start());
                long limit = byteBuf.limit();
                int startLine = 0;
                byte currentByte = 0;

                while ((startLine = byteBuf.position()) < limit) {
                    int currentPos = startLine;
                    int byteIndex = 0;
                    byte[] stationBytes = new byte[64];

                    while (currentPos < partitionEnd && (currentByte = byteBuf.get(currentPos++)) != ';') {
                        stationBytes[byteIndex++] = currentByte;
                    }

                    double temp = 0;
                    int negative = 1;
                    core: while (currentPos < partitionEnd && (currentByte = byteBuf.get(currentPos++)) != '\n') {
                        switch (currentByte) {
                            case '-':
                                negative = -1;
                            case '.':
                                break;
                            case '\r':
                                currentPos++;
                                break core;
                            default:
                                temp = 10 * temp + (currentByte - '0');
                        }
                    }

                    temp *= negative;
                    double finalTemp = temp / 10.0;

                    String stationStr = new String(stationBytes, StandardCharsets.UTF_8).trim();

                    ResultRecord current = new ResultRecord(finalTemp);
                    ResultRecord existing = concurrentMap.getOrDefault(stationStr, null);
                    if (existing != null) {
                        current = mergeResultRecords(existing, current);
                    }
                    concurrentMap.put(stationStr, current);

                    byteBuf.position(currentPos);
                }
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        });

        System.out.println(new TreeMap<>(concurrentMap));
        System.out.println("Exec time=" + (System.currentTimeMillis() - start));
    }

    private static ResultRecord mergeResultRecords(ResultRecord v, ResultRecord value) {
        return mergeResultRecords(v, value.min, value.max, value.sum, value.count);
    }

    private static ResultRecord mergeResultRecords(ResultRecord r, double min, double max, double sum, long count) {
        r.min = Math.min(r.min, min);
        r.max = Math.max(r.max, max);
        r.sum += sum;
        r.count += count;
        return r;
    }

    private static List<FilePartition> getFilePartitions(File file) throws IOException {
        int partitionCount = Runtime.getRuntime().availableProcessors();
        long fileSize = file.length();
        long partitionSize = fileSize / partitionCount;

        List<FilePartition> partitions = new ArrayList<>();
        try (RandomAccessFile raFile = new RandomAccessFile(file, "r")) {
            for (int i = 0; i < partitionCount; i++) {
                long start = i * partitionSize;
                long end = (i == partitionCount - 1) ? fileSize : start + partitionSize;
                start = findPartitionBoundry(raFile, (i == 0), start, end);
                end = findPartitionBoundry(raFile, (i == partitionCount - 1), end, fileSize);

                partitions.add(new FilePartition(start, end));
            }
        }

        return partitions;
    }

    private static long findPartitionBoundry(RandomAccessFile file, boolean skipSegment, long start, long fileEnd) throws IOException {
        if (!skipSegment) {
            file.seek(start);
            while (start < fileEnd) {
                start++;
                if (file.read() == '\n')
                    break;
            }
        }
        return start;
    }
}

class ResultRecord {
    double min;
    double max;
    double sum;
    long count;

    ResultRecord(double value) {
        min = max = sum = value;
        this.count = 1;
    };

    public String toString() {
        return round(min) + "/" + round(sum / count) + "/" + round(max);
    }

    private double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

}

record FilePartition(long start, long end) {
}
