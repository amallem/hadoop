
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChessCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, DoubleWritable>{

        public static final int WHITE_WON = 0;
        public static final int BLACK_WON = 2;
        public static final int DRAW = 1;

        private final static DoubleWritable one = new DoubleWritable(1.0);
        private Text word = new Text();
        private Text total = new Text("A");

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            PGNGame game = (PGNGame) key;
            System.out.println("Result......."+game.getResult());
            switch (game.getResult()){
                case WHITE_WON:
                    word.set("White");
                    break;
                case BLACK_WON:
                    word.set("Black");
                    break;
                case DRAW:
                    word.set("Draw");
                    break;
                default:
                    word.set("Invalid");
                    break;
            }
            context.write(word,one);
            context.write(total,one);
        }
    }

    public static class ChessCombiner
            extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,DoubleWritable,Text,Text> {

        private DoubleWritable result = new DoubleWritable();
        private HashMap<Text,Double> resultList = new HashMap<>();
        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            System.out.println("Values put to resultList......" + key.toString() + " with value = " + sum);
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
        Job job = Job.getInstance(conf, "Chess count");
        job.setJarByClass(ChessCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(ChessCombiner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(PGNInputFormat.class);
        job.setNumReduceTasks(4);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}