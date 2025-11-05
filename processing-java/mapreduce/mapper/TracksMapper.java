package mapper;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TracksMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text albumId = new Text();
    private DoubleWritable rankValue = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Ignorer l'en-tête
        if (key.get() == 0 && value.toString().contains("track_id")) return;

        String[] fields = value.toString().split(",");
        if (fields.length < 6) return; // sécurité

        albumId.set(fields[0]);             // album_id
        double rank = Double.parseDouble(fields[4]); // rank
        rankValue.set(rank);

        context.write(albumId, rankValue);
    }
}
