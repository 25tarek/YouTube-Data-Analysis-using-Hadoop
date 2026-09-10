import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

public class Top10Channels {

    public static class ChannelMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private final Text outKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.startsWith("video_id\t")) {
                return;
            }

            String[] fields = line.split("\t", -1);

            if (fields.length < 9) {
                return;
            }

            String channelTitle = fields[3].trim();
            String channelId = fields[8].trim();

            if (!channelId.isEmpty()) {
                outKey.set(channelId + "\t" + channelTitle);
                context.write(outKey, ONE);
            }
        }
    }

    public static class ChannelReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key,
                              Iterable<IntWritable> values,
                              Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static class RankingMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text channel = new Text();
        private final IntWritable count = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.isEmpty()) {
                return;
            }

            String[] fields = line.split("\t", -1);

            if (fields.length < 3) {
                return;
            }

            try {
                String channelId = fields[0];
                String channelTitle = fields[1];
                int total = Integer.parseInt(fields[2]);

                channel.set(channelId + "\t" + channelTitle);
                count.set(total);

                context.write(channel, count);

            } catch (NumberFormatException e) {
                // skip malformed rows
            }
        }
    }

    static class ChannelCount {
        String channelId;
        String channelTitle;
        int count;

        ChannelCount(String channelId, String channelTitle, int count) {
            this.channelId = channelId;
            this.channelTitle = channelTitle;
            this.count = count;
        }
    }

    public static class Top10Reducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final List<ChannelCount> channels =
                new ArrayList<ChannelCount>();

        @Override
        protected void reduce(Text key,
                              Iterable<IntWritable> values,
                              Context context) {

            String[] parts = key.toString().split("\t", -1);

            String channelId = parts.length > 0 ? parts[0] : "";
            String channelTitle = parts.length > 1 ? parts[1] : "";

            for (IntWritable value : values) {
                channels.add(
                        new ChannelCount(
                                channelId,
                                channelTitle,
                                value.get()
                        )
                );
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            channels.sort(
                    new Comparator<ChannelCount>() {
                        @Override
                        public int compare(ChannelCount a,
                                           ChannelCount b) {

                            int cmp = Integer.compare(
                                    b.count,
                                    a.count
                            );

                            if (cmp != 0) {
                                return cmp;
                            }

                            return a.channelId.compareTo(b.channelId);
                        }
                    }
            );

            int limit = Math.min(10, channels.size());

            for (int i = 0; i < limit; i++) {

                ChannelCount c = channels.get(i);

                context.write(
                        new Text(
                                c.channelId + "\t" +
                                c.channelTitle
                        ),
                        new IntWritable(c.count)
                );
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println(
                    "Usage: Top10Channels <input> <countOutput> <top10Output>"
            );
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job countJob = Job.getInstance(
                conf,
                "YouTube Channel Video Count"
        );

        countJob.setJarByClass(Top10Channels.class);

        countJob.setMapperClass(ChannelMapper.class);
        countJob.setReducerClass(ChannelReducer.class);

        countJob.setOutputKeyClass(Text.class);
        countJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(
                countJob,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                countJob,
                new Path(args[1])
        );

        boolean firstSuccess =
                countJob.waitForCompletion(true);

        if (!firstSuccess) {
            System.exit(1);
        }

        Job rankingJob = Job.getInstance(
                conf,
                "Top 10 YouTube Channels"
        );

        rankingJob.setJarByClass(Top10Channels.class);

        rankingJob.setMapperClass(RankingMapper.class);
        rankingJob.setReducerClass(Top10Reducer.class);

        rankingJob.setNumReduceTasks(1);

        rankingJob.setMapOutputKeyClass(Text.class);
        rankingJob.setMapOutputValueClass(IntWritable.class);

        rankingJob.setOutputKeyClass(Text.class);
        rankingJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(
                rankingJob,
                new Path(args[1])
        );

        FileOutputFormat.setOutputPath(
                rankingJob,
                new Path(args[2])
        );

        System.exit(
                rankingJob.waitForCompletion(true)
                        ? 0
                        : 1
        );
    }
}