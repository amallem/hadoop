
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by anirudh on 12/8/15.
 */
public class PGNInputFormat extends FileInputFormat<PGNGame, Text> {

    @Override
    public RecordReader<PGNGame, Text> createRecordReader(InputSplit inputSplit,
                                                                        TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return new PGNRecordReader();
    }
}
