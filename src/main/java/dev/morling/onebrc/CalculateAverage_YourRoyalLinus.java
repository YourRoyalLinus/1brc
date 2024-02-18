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
    /*
     * This solution was heavily inspired by CalculateAverage_spullara
     *
     * * My results on this computer:
     *
     * CalculateAverage_spullara: 0m2.013s
     * CalculateAverage_YourRoyalLinus: 0m26.692s
     * CalculateAverage: 3m22.363s
     *
     */

    private static final String FILE = "./measurements.txt";
    private static final ConcurrentHashMap<String, ResultRecord> concurrentMap = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException {
        File file = new File(FILE);
        long start = System.currentTimeMillis();

        getFilePartitions(file).parallelStream().forEach(partition -> {
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
