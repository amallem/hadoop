package chess.partB;

import chess.PGNInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by anirudh on 12/10/15.
 */
public class PlayerCountFinal {

    public static class PlayerStatsMapper
            extends Mapper<Object, Text, Text, Text> {

        private PlayerStats currentPlayer = new PlayerStats();
        private int lineCounter = 0;

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String[] player = {itr.nextToken(), itr.nextToken()};
            switch (lineCounter) {
                case 0:
                    currentPlayer.setPlayerName(player[0]);
                    currentPlayer.setPlayerColour(player[1]);
                    currentPlayer.setTotalGames(Double.parseDouble(itr.nextToken()));
                    break;
                case 1:
                case 2:
                case 3:
                    currentPlayer.setGameStats(itr.nextToken(), itr.nextToken());
                    break;
                default:
                    currentPlayer.setPercentages();
                    writePlayerToContext(context);
                    currentPlayer.reset();
                    break;
            }
        }

        private void writePlayerToContext(Context context) throws IOException, InterruptedException {
            Text key = new Text(currentPlayer.getPlayerName() + "\t"
                    + currentPlayer.getPlayerColour());
            Text value = new Text(currentPlayer.getGamesWon() + "\t"
                    + currentPlayer.getGamesLost() + "\t"
                    + currentPlayer.getGamesDraw());
            context.write(key, value);
        }
    }

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Player Stats");
        job.setJarByClass(PlayerCount.class);
        job.setMapperClass(PlayerStatsMapper.class);
        job.setReducerClass(Reducer.class);
        job.setInputFormatClass(NLineInputFormat.class);
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 100);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
