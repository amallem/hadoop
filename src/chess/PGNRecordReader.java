package chess;

import chesspresso.game.Game;
import chesspresso.pgn.PGNReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by anirudh on 12/8/15.
 */
public class PGNRecordReader extends RecordReader<PGNGame, Text> {

    private Path file = null;
    private Configuration jobConfig;
    private Text value = new Text("A");
    private PGNGame game = null;
    private PGNReader reader = null;
    private InputStream gameFileInputStream = null;

    public PGNRecordReader(){}

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        file = split.getPath();
        jobConfig = taskAttemptContext.getConfiguration();
        gameFileInputStream = FileSystem.get(jobConfig).open(file);
        reader = new PGNReader(gameFileInputStream,null);

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        try {
            Game chessGame = reader.parseGame();
            if(chessGame != null) {
                game = new PGNGame(chessGame.getWhite(),
                        chessGame.getBlack(),
                        chessGame.getTotalNumOfPlies(),
                        chessGame.getResult());
            }else{
                game = null;
            }
        }catch(Exception e){
            e.printStackTrace();
            throw new IOException(e);
        } finally{
            if(gameFileInputStream == null){
                gameFileInputStream.close();
            }
        }
        if(game == null){
            return false;
        }
        return true;
    }

    @Override
    public PGNGame getCurrentKey() throws IOException, InterruptedException {
        return game;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
