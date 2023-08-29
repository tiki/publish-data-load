/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.data;

import com.mytiki.ocean.common.Initialize;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class AvroToParquet {
    private final S3Client client;
    private final String readBucket;
    private final String writeBucket;
    private final String credentialsProvider;

    public AvroToParquet(Properties properties) {
        client = S3Client.builder()
                .region(Region.of(properties.getProperty("s3-region")))
                .build();
        readBucket = properties.getProperty("s3-read-bucket");
        writeBucket = properties.getProperty("s3-write-bucket");
        credentialsProvider = properties.getProperty("s3-credentials-provider");
    }

    public static AvroToParquet load() {
        return AvroToParquet.load("avro.properties");
    }

    public static AvroToParquet load(String filename) {
        Properties properties = Initialize.properties(filename);
        return new AvroToParquet(properties);
    }

    public List<GenericRecord> read(String key) throws IOException {
        List<GenericRecord> records = new ArrayList<>();
        ResponseInputStream<GetObjectResponse> inputStream = client.getObject(
                GetObjectRequest.builder().bucket(readBucket).key(key).build(),
                ResponseTransformer.toInputStream()
        );

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileStream<GenericRecord> fileStream = new DataFileStream<>(inputStream, datumReader);
        while (fileStream.hasNext()) records.add(fileStream.next());

        return records;
    }

    public void write(String keyPrefix, List<GenericRecord> records) throws IOException {
        if(records == null || records.isEmpty()) return;

        String key = String.join("/", keyPrefix, UUID.randomUUID() + ".parquet");
        Schema schema = records.get(0).getSchema();

        Path path = new Path(String.join("/", "s3a:/", writeBucket, key));
        Configuration conf = new Configuration();
        conf.set("fs.s3a.aws.credentials.provider", credentialsProvider);

        ParquetWriter<Object> writer = AvroParquetWriter
                .builder(path)
                .withSchema(schema)
                .withConf(conf)
                .build();

        for (GenericRecord record : records) writer.write(record);
        writer.close();
    }
}
