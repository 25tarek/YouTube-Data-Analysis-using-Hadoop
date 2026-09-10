import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class YoutubePreprocessor {

    static class VideoRecord {
        String videoId;
        String trendingDate;
        String title;
        String channelTitle;
        String views;
        String likes;
        String categoryId;
        String comments;
        String channelId;

        VideoRecord(CSVRecord r) {
            this.videoId = clean(r.get("video_id"));
            this.trendingDate = clean(r.get("trending_date"));
            this.title = clean(r.get("title"));
            this.channelTitle = clean(r.get("channel_title"));
            this.views = clean(r.get("views"));
            this.likes = clean(r.get("likes"));
            this.categoryId = clean(r.get("category_id"));
            this.comments = clean(r.get("comments"));
            this.channelId = clean(r.get("channel_id"));
        }

        String toTsv() {
            return String.join("\t",
                    videoId,
                    trendingDate,
                    title,
                    channelTitle,
                    views,
                    likes,
                    categoryId,
                    comments,
                    channelId
            );
        }
    }

    static String clean(String value) {
        if (value == null) return "";
        return value
                .replace("\t", " ")
                .replace("\r", " ")
                .replace("\n", " ")
                .trim();
    }

    static LocalDate parseTrendingDate(String s) {
    try {
        String[] parts = s.trim().split("\\.");

        if (parts.length != 3) {
            return LocalDate.MIN;
        }

        int year = 2000 + Integer.parseInt(parts[0]);
        int day = Integer.parseInt(parts[1]);
        int month = Integer.parseInt(parts[2]);

        return LocalDate.of(year, month, day);

    } catch (Exception e) {
        return LocalDate.MIN;
    }
}

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Usage:");
            System.out.println("java YoutubePreprocessor <input.csv> <output.tsv>");
            return;
        }

        String inputPath = args[0];
        String outputPath = args[1];

        Map<String, VideoRecord> latestVideos = new HashMap<>();

        long totalRows = 0;
        long skippedRows = 0;

        try (
                Reader reader = new InputStreamReader(
                        new FileInputStream(inputPath),
                        StandardCharsets.UTF_8
                );

                CSVParser parser = CSVFormat.DEFAULT
                        .builder()
                        .setHeader()
                        .setSkipHeaderRecord(true)
                        .setIgnoreEmptyLines(true)
                        .build()
                        .parse(reader)
        ) {

            for (CSVRecord record : parser) {
                totalRows++;

                try {
                    VideoRecord current = new VideoRecord(record);

                    if (current.videoId.isEmpty()) {
                        skippedRows++;
                        continue;
                    }

                    VideoRecord existing =
                            latestVideos.get(current.videoId);

                    if (existing == null) {
                        latestVideos.put(
                                current.videoId,
                                current
                        );
                    } else {
                        LocalDate existingDate =
                                parseTrendingDate(
                                        existing.trendingDate
                                );

                        LocalDate currentDate =
                                parseTrendingDate(
                                        current.trendingDate
                                );

                        if (currentDate.isAfter(existingDate)) {
                            latestVideos.put(
                                    current.videoId,
                                    current
                            );
                        }
                    }

                } catch (Exception e) {
                    skippedRows++;
                }
            }
        }

        try (
                BufferedWriter writer =
                        new BufferedWriter(
                                new OutputStreamWriter(
                                        new FileOutputStream(outputPath),
                                        StandardCharsets.UTF_8
                                )
                        )
        ) {

            writer.write(
                    "video_id\ttrending_date\ttitle\tchannel_title\tviews\tlikes\tcategory_id\tcomments\tchannel_id"
            );
            writer.newLine();

            for (VideoRecord record : latestVideos.values()) {
                writer.write(record.toTsv());
                writer.newLine();
            }
        }

        System.out.println("=================================");
        System.out.println("Preprocessing completed");
        System.out.println("=================================");
        System.out.println("Total raw rows     : " + totalRows);
        System.out.println("Skipped rows       : " + skippedRows);
        System.out.println("Unique videos kept : " + latestVideos.size());
        System.out.println("Output file        : " + outputPath);
        System.out.println("=================================");
    }
}