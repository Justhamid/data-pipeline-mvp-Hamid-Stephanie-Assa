package com.ynov.pipeline;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Entrée : lignes TSV
 *   genre_id, genre_name, track_id, title, artist_name, rank
 *
 * Sortie Mapper :
 *   key   = genre_id#track_id
 *   value = genre_name\ttitle\tartist_name\trank
 */
public class GenreTopTracksMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        if (line.trim().isEmpty()) {
            return;
        }

        String[] parts = line.split("\t", -1);
        if (parts.length < 6) {
            // ligne invalide -> on ignore
            return;
        }

        String genreId    = parts[0];
        String genreName  = parts[1];
        String trackId    = parts[2];
        String title      = parts[3];
        String artistName = parts[4];
        String rank       = parts[5];

        if (trackId == null || trackId.isEmpty() || "null".equalsIgnoreCase(trackId)) {
            return;
        }

        String k = genreId + "#" + trackId;
        String v = genreName + "\t" + title + "\t" + artistName + "\t" + rank;

        outKey.set(k);
        outVal.set(v);
        context.write(outKey, outVal);
    }
}
