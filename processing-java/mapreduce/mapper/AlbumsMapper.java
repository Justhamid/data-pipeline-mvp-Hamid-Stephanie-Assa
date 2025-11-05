package mapper;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AlbumsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text artistId = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Ignorer l'en-tête
        if (key.get() == 0 && value.toString().contains("album_id")) return;

        String[] fields = value.toString().split(",");
        if (fields.length < 2) return;  // sécurité

        artistId.set(fields[1]);  // artist_id
        context.write(artistId, one);  // émettre (artist_id, 1)
    }
}
