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

public class Top10Categories {

    public static class CategoryMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private final Text category = new Text();

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

            String categoryId = fields[6].trim();

            if (!categoryId.isEmpty()) {
                category.set(categoryId);
                context.write(category, ONE);
            }
        }
    }

    public static class CategoryReducer
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

        private final Text category = new Text();
        private final IntWritable count = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split("\t");

            if (parts.length != 2) {
                return;
            }

            try {
                category.set(parts[0]);
                count.set(Integer.parseInt(parts[1]));
                context.write(category, count);

            } catch (NumberFormatException e) {
                // Skip malformed rows
            }
        }
    }

    public static class Top10Reducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final List<CategoryCount> categories =
                new ArrayList<CategoryCount>();

        @Override
        protected void reduce(Text key,
                              Iterable<IntWritable> values,
                              Context context) {

            for (IntWritable value : values) {
                categories.add(
                        new CategoryCount(
                                key.toString(),
                                value.get()
                        )
                );
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            categories.sort(
                    new Comparator<CategoryCount>() {
                        @Override
                        public int compare(CategoryCount a,
                                           CategoryCount b) {

                            int countComparison =
                                    Integer.compare(
                                            b.count,
                                            a.count
                                    );

                            if (countComparison != 0) {
                                return countComparison;
                            }

                            return a.categoryId
                                    .compareTo(b.categoryId);
                        }
                    }
            );

            int limit = Math.min(10, categories.size());

            for (int i = 0; i < limit; i++) {
                CategoryCount c = categories.get(i);

                context.write(
                        new Text(c.categoryId),
                        new IntWritable(c.count)
                );
            }
        }
    }

    static class CategoryCount {

        String categoryId;
        int count;

        CategoryCount(String categoryId, int count) {
            this.categoryId = categoryId;
            this.count = count;
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println(
                    "Usage: Top10Categories <input> <countOutput> <top10Output>"
            );
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job countJob =
                Job.getInstance(
                        conf,
                        "YouTube Category Video Count"
                );

        countJob.setJarByClass(Top10Categories.class);

        countJob.setMapperClass(CategoryMapper.class);
        countJob.setReducerClass(CategoryReducer.class);

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

        Job rankingJob =
                Job.getInstance(
                        conf,
                        "Top 10 YouTube Categories"
                );

        rankingJob.setJarByClass(Top10Categories.class);

        rankingJob.setMapperClass(RankingMapper.class);
        rankingJob.setReducerClass(Top10Reducer.class);

        /*
         * Important:
         * only one reducer so that one reducer sees
         * all category totals and produces a global Top 10.
         */
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