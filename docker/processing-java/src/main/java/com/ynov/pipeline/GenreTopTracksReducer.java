package com.ynov.pipeline;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Réduit par (genre_id, track_id) et sélectionne le meilleur rang (min(position)).
 *
 * Sortie Reducer (ligne TSV finale) :
 *   genre_id, genre_name, track_id, title, artist_name, best_rank
 *
 * key   = genre_id#track_id (ignoré dans la sortie finale)
 * value = genre_name\ttitle\tartist_name\trank
 */
public class GenreTopTracksReducer extends Reducer<Text, Text, NullWritable, Text> {

    private final Text outVal = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String genreId = null;
        String genreName = "unknown";
        String trackId = null;
        String title = "unknown";
        String artistName = "unknown";
        int bestRank = Integer.MAX_VALUE;

        String keyStr = key.toString();
        String[] keyParts = keyStr.split("#", 2);
        if (keyParts.length == 2) {
            genreId = keyParts[0];
            trackId = keyParts[1];
        }

        for (Text tv : values) {
            String[] parts = tv.toString().split("\t", -1);
            if (parts.length < 4) continue;

            String gName = parts[0];
            String tTitle = parts[1];
            String aName = parts[2];
            String rankStr = parts[3];

            int rank;
            try {
                rank = Integer.parseInt(rankStr.trim());
            } catch (NumberFormatException e) {
                continue;
            }

            // On garde le meilleur (plus petit) rang
            if (rank < bestRank) {
                bestRank = rank;
                if (gName != null && !gName.isEmpty()) genreName = gName;
                if (tTitle != null && !tTitle.isEmpty()) title = tTitle;
                if (aName != null && !aName.isEmpty()) artistName = aName;
            }
        }

        if (genreId == null || trackId == null || bestRank == Integer.MAX_VALUE) {
            return; // rien à écrire
        }

        String outLine = String.join("\t",
                genreId,
                genreName,
                trackId,
                title,
                artistName,
                String.valueOf(bestRank)
        );

        outVal.set(outLine);
        context.write(NullWritable.get(), outVal);
    }
}
