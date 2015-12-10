import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by anirudh on 12/9/15.
 */
public class ChessCountFinal {

    public static class SecondMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private DoubleWritable result = null;
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException{
            word.set(key.toString());
            result = new DoubleWritable(Double.parseDouble(value.toString()));
            context.write(word,result);
        }
    }


    public static class SecondReducer extends Reducer<Text, DoubleWritable, Text, Text> {

        private DoubleWritable result = new DoubleWritable();
        private HashMap<Text,Double> resultList = new HashMap<>();
        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            System.out.println("Values put to resultListFinal......" + key.toString() + " with value = " + sum);
            resultList.put(new Text(key.toString()), sum);
            System.out.println("ResultList....." + resultList);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
            Text total = new Text("A");
            System.out.println("Inside CleanUp....." + resultList);
            double totalGames = resultList.get(total);
            resultList.remove(total);
            Iterator it = resultList.entrySet().iterator();
            double percent;
            Map.Entry pair;
            while(it.hasNext()){
                pair = (Map.Entry)it.next();
                percent = (double)pair.getValue() / totalGames ;
                result.set(percent);
                Text finalVal = new Text(new Double((double)pair.getValue()).toString() + " " + result.toString());
                context.write((Text) pair.getKey(), finalVal);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Chess count final");
        job.setJarByClass(ChessCountFinal.class);
        job.setMapperClass(SecondMapper.class);
        job.setReducerClass(SecondReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
