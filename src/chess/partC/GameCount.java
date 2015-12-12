package chess.partC;

import chess.PGNGame;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by anirudh on 12/11/15.
 */
public class GameCount {

    public static class MovesMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        public void map(Object key, Text value, Context context) {

        }

        public static void main(String args[]) throws Exception {

        }
    }
}
