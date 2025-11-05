package mapper;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ArtistPerformanceMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text artistId = new Text();
    private DoubleWritable valueOut = new DoubleWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // CSV : artist_id, artist_name, nb_fan, avg_album_rank
        String[] fields = value.toString().split(",");
        if (fields.length < 4) return;

        artistId.set(fields[0]);
        double score = Double.parseDouble(fields[3]); // avg_album_rank
        valueOut.set(score);

        context.write(artistId, valueOut);
    }
}
