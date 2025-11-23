package com.ynov.pipeline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.net.URI;
import java.sql.*;
import java.util.Properties;

public class ExportToHdfs {

    public static void main(String[] args) throws Exception {
        // Charge application.properties
        Properties app = new Properties();
        app.load(ExportToHdfs.class.getClassLoader()
                .getResourceAsStream("application.properties"));

        String pgHost = app.getProperty("pg.host", "postgres");
        String pgPort = app.getProperty("pg.port", "5432");
        String pgDb   = app.getProperty("pg.db", "datapipeline");
        String pgUser = app.getProperty("pg.user", "postgres");
        String pgPass = app.getProperty("pg.password", "postgres");
        String baseQuery = app.getProperty("sql.query");
        int limit = Integer.parseInt(app.getProperty("limit.rows", "5000"));
        String query = "SELECT * FROM (" + baseQuery + ") q LIMIT " + limit;

        String hdfsPath = app.getProperty("hdfs.input.path", "/data/phase2/input/genre_tracks.tsv");

        String jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s", pgHost, pgPort, pgDb);

        System.out.println("[ExportToHdfs] JDBC URL = " + jdbcUrl);
        System.out.println("[ExportToHdfs] Query    = " + query);
        System.out.println("[ExportToHdfs] HDFS out = " + hdfsPath);

        Class.forName("org.postgresql.Driver");

        try (Connection conn = DriverManager.getConnection(jdbcUrl, pgUser, pgPass);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            // Config Hadoop : on utilise fs.defaultFS déjà configuré dans le container,
            // mais on force au cas où.
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://namenode:9000");

            FileSystem fs = FileSystem.get(new URI("hdfs://namenode:9000"), conf);

            Path path = new Path(hdfsPath);
            // On recrée le fichier à chaque export
            if (fs.exists(path)) {
                fs.delete(path, true);
            }

            Path parent = path.getParent();
            if (parent != null && !fs.exists(parent)) {
                fs.mkdirs(parent);
            }

            try (FSDataOutputStream out = fs.create(path);
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {

                int count = 0;
                while (rs.next()) {
                    // colonnes issues du SELECT :
                    // genre_id, genre_name, track_id, title, artist_name, rank
                    String genreId   = String.valueOf(rs.getObject("genre_id"));
                    String genreName = rs.getString("genre_name");
                    String trackId   = String.valueOf(rs.getLong("track_id"));
                    String title     = rs.getString("title");
                    String artist    = rs.getString("artist_name");
                    String rank      = String.valueOf(rs.getInt("rank"));

                    if (genreId == null || "null".equals(genreId)) {
                        genreId = "-1";
                    }
                    if (genreName == null) genreName = "unknown";
                    if (title == null) title = "unknown";
                    if (artist == null) artist = "unknown";

                    String line = String.join("\t",
                            genreId, genreName, trackId, title, artist, rank);
                    writer.write(line);
                    writer.newLine();
                    count++;
                }
                writer.flush();
                System.out.println("[ExportToHdfs] Exported rows = " + count);
            }
        }

        System.out.println("[ExportToHdfs] Done.");
    }
}
