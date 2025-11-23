package com.ynov.pipeline;

import org.apache.spark.sql.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.functions;

import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

public class SparkJob {

    public static void main(String[] args) throws Exception {
        // --- Spark session (local[*] par défaut : pratique en docker)
        SparkConf conf = new SparkConf()
                .setAppName("Phase2-Spark-HBase")
                .setIfMissing("spark.master", "local[*]");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        // --- Charge la conf application.properties
        java.io.InputStream is = SparkJob.class.getClassLoader()
                .getResourceAsStream("application.properties");
        java.util.Properties app = new java.util.Properties();
        app.load(is);

        // PostgreSQL
        String pgHost = app.getProperty("pg.host", "postgres");
        String pgPort = app.getProperty("pg.port", "5432");
        String pgDb   = app.getProperty("pg.db", "datapipeline");
        String pgUser = app.getProperty("pg.user", "postgres");
        String pgPass = app.getProperty("pg.password", "postgres");
        String jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s", pgHost, pgPort, pgDb);

        String baseQuery = app.getProperty("sql.query");
        int limit = Integer.parseInt(app.getProperty("limit.rows", "5000"));
        String query = "SELECT * FROM (" + baseQuery + ") q LIMIT " + limit;

        // HBase
        String zkQuorum = app.getProperty("hbase.zookeeper.quorum", "zookeeper");
        String zkPort   = app.getProperty("hbase.zookeeper.property.clientPort", "2181");
        String table    = app.getProperty("hbase.table", "tracks_by_genre");
        String cf       = app.getProperty("hbase.column.family", "cf");

        // --- Lecture JDBC → DataFrame
        Properties jdbcProps = new Properties();
        jdbcProps.put("user", pgUser);
        jdbcProps.put("password", pgPass);
        jdbcProps.put("driver", "org.postgresql.Driver");

        Dataset<Row> df = spark.read()
                .jdbc(jdbcUrl, "(" + query + ") x", jdbcProps);

        // Exemple de transformation : garder les colonnes utiles + clef de ligne
        // RowKey = genre_id#track_id
        df = df.withColumn("genre_id", functions.col("genre_id").cast(DataTypes.IntegerType))
               .withColumn("track_id", functions.col("track_id").cast(DataTypes.LongType))
               .withColumn("rank", functions.col("rank").cast(DataTypes.IntegerType))
               .withColumn("rowkey",
                       functions.concat_ws("#",
                               functions.coalesce(functions.col("genre_id"), functions.lit(-1)),
                               functions.coalesce(functions.col("track_id"), functions.lit(-1)))
               )
               .select("rowkey", "genre_id", "genre_name", "track_id", "title", "artist_name", "rank")
               .na().fill("unknown", new String[]{"genre_name", "title", "artist_name"})
               .na().fill(-1, new String[]{"genre_id", "rank"})
               .cache();

        long count = df.count();
        System.out.println("Rows to write to HBase: " + count);

        // --- Écriture vers HBase (foreachPartition)
        final String zkQ = zkQuorum;
        final String zkP = zkPort;
        final String hTable = table;
        final String family = cf;

        df.foreachPartition((ForeachPartitionFunction<Row>) (Iterator<Row> it) -> {
            Configuration hconf = HBaseConfiguration.create();
            hconf.set("hbase.zookeeper.quorum", zkQ);
            hconf.set("hbase.zookeeper.property.clientPort", zkP);

            try (Connection conn = ConnectionFactory.createConnection(hconf)) {
                TableName tname = TableName.valueOf(hTable);
                try (Table htable = conn.getTable(tname)) {
                    while (it.hasNext()) {
                        Row r = it.next();
                        String rowkey = r.getAs("rowkey");
                        Put put = new Put(rowkey.getBytes());

                        // Colonnes
                        put.addColumn(family.getBytes(), "genre_id".getBytes(),
                                String.valueOf(r.<Integer>getAs("genre_id")).getBytes());
                        put.addColumn(family.getBytes(), "genre_name".getBytes(),
                                r.<String>getAs("genre_name").getBytes());
                        put.addColumn(family.getBytes(), "track_id".getBytes(),
                                String.valueOf(r.<Long>getAs("track_id")).getBytes());
                        put.addColumn(family.getBytes(), "title".getBytes(),
                                r.<String>getAs("title").getBytes());
                        put.addColumn(family.getBytes(), "artist_name".getBytes(),
                                r.<String>getAs("artist_name").getBytes());
                        put.addColumn(family.getBytes(), "rank".getBytes(),
                                String.valueOf(r.<Integer>getAs("rank")).getBytes());

                        htable.put(put);
                    }
                }
            }
        });

        System.out.println("Write finished.");
        spark.stop();
    }
}
