package chess.partC;

import chess.PGNGame;
import chess.PGNInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by anirudh on 12/11/15.
 */
public class GameCount {

    public static class MovesMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text bucket1 = new Text("-1");
        private Text bucket2 = new Text("-2");
        private Text bucket3 = new Text("-3");
        private Text bucket4 = new Text("-4");
        private Text bucket5 = new Text("-5");
        private DoubleWritable one = new DoubleWritable(1.0);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            PGNGame game = (PGNGame) key;
            context.write(new Text(game.getPlyCount() + ""), one);
            context.write(bucket1, one);
            context.write(bucket2, one);
            context.write(bucket3, one);
            context.write(bucket4, one);
            context.write(bucket5, one);
        }


    }

    public static class GameCountCombiner
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static class GameCountPartitioner
            extends Partitioner<Text, DoubleWritable> {

        @Override
        public int getPartition(Text text, DoubleWritable doubleWritable, int i) {
            int value = Integer.parseInt(text.toString());
            if (value < 0) {
                return ((value * -1) - 1);
            } else {
                if (value > 0 && value < 50) {
                    return 0;
                } else if (value > 50 && value < 100) {
                    return 1;
                } else if (value > 100 && value < 200) {
                    return 2;
                } else if (value > 200 && value < 500) {
                    return 3;
                } else {
                    return 4;
                }
            }
        }
    }

    public static class GameCountReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private boolean isFirst = true;
        private double totalGames;

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            System.out.println(key.toString() + " -------> " + Double.toString(sum));
            if (isFirst) {
                totalGames = sum;
                isFirst = false;
                return;
            }
            double result = sum / totalGames;
            context.write(key, new DoubleWritable(result));
        }
    }

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Game Stats");
        job.setJarByClass(GameCount.class);
        job.setMapperClass(MovesMapper.class);
        job.setReducerClass(GameCountReducer.class);
        job.setCombinerClass(GameCountCombiner.class);
        job.setPartitionerClass(GameCountPartitioner.class);
        job.setNumReduceTasks(5);

        job.setInputFormatClass(PGNInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
