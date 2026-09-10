import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Top10ViewedVideos {

    static class VideoInfo {
        String videoId;
        String title;
        String channelTitle;
        long views;

        VideoInfo(String videoId, String title,
                  String channelTitle, long views) {
            this.videoId = videoId;
            this.title = title;
            this.channelTitle = channelTitle;
            this.views = views;
        }
    }

    public static class ViewedMapper
            extends Mapper<LongWritable, Text, NullWritable, Text> {

        private final List<VideoInfo> topVideos =
                new ArrayList<VideoInfo>();

        @Override
        protected void map(LongWritable key, Text value, Context context) {

            String line = value.toString();

            if (line.startsWith("video_id\t")) {
                return;
            }

            String[] fields = line.split("\t", -1);

            if (fields.length < 9) {
                return;
            }

            try {
                String videoId = fields[0].trim();
                String title = fields[2].trim();
                String channelTitle = fields[3].trim();
                long views = Long.parseLong(fields[4].trim());

                topVideos.add(
                        new VideoInfo(
                                videoId,
                                title,
                                channelTitle,
                                views
                        )
                );

            } catch (Exception e) {
                // skip malformed rows
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            topVideos.sort(
                    new Comparator<VideoInfo>() {
                        @Override
                        public int compare(VideoInfo a, VideoInfo b) {
                            int cmp = Long.compare(b.views, a.views);

                            if (cmp != 0) {
                                return cmp;
                            }

                            return a.videoId.compareTo(b.videoId);
                        }
                    }
            );

            int limit = Math.min(10, topVideos.size());

            for (int i = 0; i < limit; i++) {
                VideoInfo v = topVideos.get(i);

                context.write(
                        NullWritable.get(),
                        new Text(
                                v.videoId + "\t" +
                                v.title + "\t" +
                                v.channelTitle + "\t" +
                                v.views
                        )
                );
            }
        }
    }

    public static class Top10Reducer
            extends Reducer<NullWritable, Text, NullWritable, Text> {

        private final List<VideoInfo> allCandidates =
                new ArrayList<VideoInfo>();

        @Override
        protected void reduce(
                NullWritable key,
                Iterable<Text> values,
                Context context) {

            for (Text value : values) {

                String[] fields =
                        value.toString().split("\t", -1);

                if (fields.length < 4) {
                    continue;
                }

                try {
                    String videoId = fields[0];
                    String title = fields[1];
                    String channelTitle = fields[2];
                    long views =
                            Long.parseLong(fields[3]);

                    allCandidates.add(
                            new VideoInfo(
                                    videoId,
                                    title,
                                    channelTitle,
                                    views
                            )
                    );

                } catch (Exception e) {
                    // skip malformed rows
                }
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            allCandidates.sort(
                    new Comparator<VideoInfo>() {
                        @Override
                        public int compare(VideoInfo a, VideoInfo b) {
                            int cmp = Long.compare(b.views, a.views);

                            if (cmp != 0) {
                                return cmp;
                            }

                            return a.videoId.compareTo(b.videoId);
                        }
                    }
            );

            int limit = Math.min(10, allCandidates.size());

            for (int i = 0; i < limit; i++) {

                VideoInfo v = allCandidates.get(i);

                context.write(
                        NullWritable.get(),
                        new Text(
                                v.videoId + "\t" +
                                v.title + "\t" +
                                v.channelTitle + "\t" +
                                v.views
                        )
                );
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                    "Usage: Top10ViewedVideos <input> <output>"
            );
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "Top 10 Most Viewed YouTube Videos"
        );

        job.setJarByClass(Top10ViewedVideos.class);

        job.setMapperClass(ViewedMapper.class);
        job.setReducerClass(Top10Reducer.class);

        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );

        System.exit(
                job.waitForCompletion(true) ? 0 : 1
        );
    }
}