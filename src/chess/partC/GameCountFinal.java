package chess.partC;

import chess.PGNInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by anirudh on 12/12/15.
 */
public class GameCountFinal {

    public static class GameCountFinalMapper extends Mapper<Object, Text, DoubleWritable, Text> {

        private DoubleWritable resultKey = new DoubleWritable();
        private Text resultValue = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String freq = itr.nextToken();
            String percent = itr.nextToken();
            resultKey.set(Double.parseDouble(percent) * 100.0);
            resultValue.set(freq);
//            System.out.println("----------->" + freq + " ----- " + percent + "<----------- ");
            context.write(resultKey, resultValue);
        }
    }

    public static class GameCountFinalReducer extends Reducer<DoubleWritable, Text, Text, Text> {
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            while (values.iterator().hasNext()) {
                context.write(values.iterator().next(), new Text(key.toString() + "%"));
            }
        }
    }

    public static void main(String args[])
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Game Stats");
        job.setJarByClass(GameCountFinal.class);
        job.setMapperClass(GameCountFinalMapper.class);
        job.setReducerClass(GameCountFinalReducer.class);
        job.setNumReduceTasks(1);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
