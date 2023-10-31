/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.publish.data.load.file;

import com.mytiki.publish.data.load.aws.AwsS3;
import com.mytiki.publish.data.load.aws.AwsSQS;
import com.mytiki.publish.data.load.utils.Props;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class FileClient {
    private final String credentialsProvider;
    private final AwsS3 s3;
    private final AwsSQS sqs;
    private final String writeBucket;
    private final String writeQueue;

    public FileClient(String credentialsProvider, String region, String writeBucket, String writeQueue) {
        this.credentialsProvider = credentialsProvider;
        this.s3 = new AwsS3(region);
        this.sqs = new AwsSQS(region);
        this.writeBucket = writeBucket;
        this.writeQueue = writeQueue;
    }

    public static FileClient load(String filename) {
        Properties properties = Props.withEnv(filename);
        return new FileClient(
                properties.getProperty("client.write.credentials.provider"),
                properties.getProperty("client.region"),
                properties.getProperty("client.write.bucket.name"),
                properties.getProperty("client.write.queue.url")
        );
    }

    public static FileClient load() {
        return load("aws.properties");
    }

    public List<GenericRecord> read(String bucket, String key) throws IOException {
        List<GenericRecord> records = new ArrayList<>();
        ResponseInputStream<GetObjectResponse> inputStream = s3.get(bucket, key);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileStream<GenericRecord> fileStream = new DataFileStream<>(inputStream, datumReader);
        while (fileStream.hasNext()) records.add(fileStream.next());
        return records;
    }

    public String write(String table, List<GenericRecord> records) throws IOException {
        Schema schema = records.get(0).getSchema();
        String key = String.join("/", table, UUID.randomUUID() + ".parquet");
        Configuration conf = new Configuration();
        conf.set("fs.s3a.aws.credentials.provider", credentialsProvider);

        OutputFile file = HadoopOutputFile.fromPath(
                new Path(String.join("/", "s3a:/", writeBucket, key)), conf);
        ParquetWriter<Object> writer = AvroParquetWriter
                .builder(file)
                .withSchema(schema)
                .withConf(conf)
                .build();

        for (GenericRecord record : records) writer.write(record);
        writer.close();

        FileMetadata metadata = new FileMetadata();
        metadata.setSize(s3.size(writeBucket, key));
        metadata.setUri(String.join("/", "s3:/", writeBucket, key));
        metadata.setCount(records.size());
        metadata.setTable(table);
        return sqs.notify(writeQueue, metadata);
    }
}
