package mapper;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AlbumPerformanceMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private Text artistId = new Text();
    private DoubleWritable avgRank = new DoubleWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // CSV : album_id, artist_id, album_title, release_date, avg_rank
        String[] fields = value.toString().split(",");
        if (fields.length < 3) return;

        artistId.set(fields[1]); // artist_id
        double rank = Double.parseDouble(fields[4]); // avg_rank
        avgRank.set(rank);

        context.write(artistId, avgRank);
    }
}
