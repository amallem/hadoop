package chess.partB;

import chess.PGNGame;
import chess.PGNInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by anirudh on 12/9/15.
 */
public class PlayerCount {

    public static class PlayerMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text resultWhite = new Text();
        private Text resultBlack = new Text();
        private Text totalBlack = new Text();
        private Text totalWhite = new Text();
        private DoubleWritable one = new DoubleWritable(1.0);
        private DoubleWritable zero = new DoubleWritable(0.0);

        private static final String WHITE = "White";
        private static final String BLACK = "Black";
        private static final String DRAW = "Draw";
        private static final String WON = "Won";
        private static final String LOSS = "Loss";
        private static final int WHITE_WON = 0;
        private static final int BLACK_WON = 2;
        private static final int DRAW_GAME = 1;

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            PGNGame game = (PGNGame) key;
            int gameResult = game.getResult();
            if (gameResult != DRAW_GAME) {
                if (gameResult == WHITE_WON) {
                    writeContext(context, game, true);
                } else {
                    writeContext(context, game, false);
                }
            } else {
                resultWhite.set(game.getWhitePlayer() + " " + WHITE + " " + DRAW);
                context.write(resultWhite, one);
                resultWhite.set(game.getWhitePlayer() + " " + WHITE + " " + WON);
                context.write(resultWhite, zero);
                resultWhite.set(game.getWhitePlayer() + " " + WHITE + " " + LOSS);
                context.write(resultWhite, zero);

                resultBlack.set(game.getBlackPlayer() + " " + BLACK + " " + DRAW);
                context.write(resultBlack, one);
                resultBlack.set(game.getBlackPlayer() + " " + BLACK + " " + WON);
                context.write(resultBlack, zero);
                resultBlack.set(game.getBlackPlayer() + " " + BLACK + " " + LOSS);
                context.write(resultBlack, zero);
            }
            totalBlack.set(game.getBlackPlayer() + " " + BLACK);
            totalWhite.set(game.getWhitePlayer() + " " + WHITE);
            context.write(totalWhite, one);
            context.write(totalBlack, one);
        }

        private void writeContext(Context context, PGNGame game, boolean whiteWon) throws IOException, InterruptedException {
            if (whiteWon) {
                resultWhite.set(game.getWhitePlayer() + " " + WHITE + " " + WON);
                context.write(resultWhite, one);
                resultWhite.set(game.getWhitePlayer() + " " + WHITE + " " + LOSS);
                context.write(resultWhite, zero);
                resultWhite.set(game.getWhitePlayer() + " " + WHITE + " " + DRAW);
                context.write(resultWhite, zero);

                resultBlack.set(game.getBlackPlayer() + " " + BLACK + " " + WON);
                context.write(resultBlack, zero);
                resultBlack.set(game.getBlackPlayer() + " " + BLACK + " " + LOSS);
                context.write(resultBlack, one);
                resultBlack.set(game.getBlackPlayer() + " " + BLACK + " " + DRAW);
                context.write(resultBlack, zero);
            } else {
                resultWhite.set(game.getWhitePlayer() + " " + WHITE + " " + WON);
                context.write(resultWhite, zero);
                resultWhite.set(game.getWhitePlayer() + " " + WHITE + " " + LOSS);
                context.write(resultWhite, one);
                resultWhite.set(game.getWhitePlayer() + " " + WHITE + " " + DRAW);
                context.write(resultWhite, zero);

                resultBlack.set(game.getBlackPlayer() + " " + BLACK + " " + WON);
                context.write(resultBlack, one);
                resultBlack.set(game.getBlackPlayer() + " " + BLACK + " " + LOSS);
                context.write(resultBlack, zero);
                resultBlack.set(game.getBlackPlayer() + " " + BLACK + " " + DRAW);
                context.write(resultBlack, zero);
            }
        }
    }

    public static class PlayerPartitioner extends Partitioner<Text, DoubleWritable> {

        @Override
        public int getPartition(Text text, DoubleWritable doubleWritable, int i) {
            StringTokenizer tokens = new StringTokenizer(text.toString());
            String partitionField = tokens.nextToken();
            return (partitionField.toLowerCase().charAt(0) - 'a');
        }
    }

    public static class PlayerReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Player Stats");
        job.setJarByClass(PlayerCount.class);
        job.setMapperClass(PlayerMapper.class);
        job.setReducerClass(PlayerReducer.class);
        job.setInputFormatClass(PGNInputFormat.class);
        job.setPartitionerClass(PlayerPartitioner.class);
        job.setNumReduceTasks(26);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
