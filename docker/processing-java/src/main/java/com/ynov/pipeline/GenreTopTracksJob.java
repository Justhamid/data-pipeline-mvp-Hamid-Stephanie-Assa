package com.ynov.pipeline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;
import java.util.Properties;

/**
 * Job MapReduce :
 *  1) lit l'input TSV (ExportToHdfs) depuis HDFS
 *  2) calcule le meilleur rang (min rank) pour chaque (genre_id, track_id)
 *  3) écrit le résultat en TSV dans HDFS (pour Hive)
 */
public class GenreTopTracksJob {

    public static void main(String[] args) throws Exception {
        // Charge application.properties pour récupérer input / output si aucun argument
        Properties app = new Properties();
        app.load(GenreTopTracksJob.class.getClassLoader()
                .getResourceAsStream("application.properties"));

        String defaultInput  = app.getProperty("hdfs.input.path", "/data/phase2/input/genre_tracks.tsv");
        String defaultOutput = app.getProperty("hdfs.output.path", "/data/phase2/output/genre_top_tracks");

        String inputPath  = (args.length > 0) ? args[0] : defaultInput;
        String outputPath = (args.length > 1) ? args[1] : defaultOutput;

        System.out.println("[GenreTopTracksJob] Input  = " + inputPath);
        System.out.println("[GenreTopTracksJob] Output = " + outputPath);

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://namenode:9000");

        // Nettoyage de l'output si déjà présent
        FileSystem fs = FileSystem.get(new URI("hdfs://namenode:9000"), conf);
        Path outPath = new Path(outputPath);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        Job job = Job.getInstance(conf, "genre_top_tracks");
        job.setJarByClass(GenreTopTracksJob.class);

        job.setMapperClass(GenreTopTracksMapper.class);
        job.setReducerClass(GenreTopTracksReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(inputPath));
        TextOutputFormat.setOutputPath(job, outPath);

        job.setNumReduceTasks(1);

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
