/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.data;

import com.mytiki.ocean.common.Initialize;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class Avro {
    private final S3Client client;
    private final String readBucket;
    private final String writeBucket;

    public Avro(Properties properties) {
        client = S3Client.builder()
                .region(Region.of(properties.getProperty("s3-region")))
                .build();
        readBucket = properties.getProperty("s3-read-bucket");
        writeBucket = properties.getProperty("s3-write-bucket");
    }

    public static Avro load() {
        return Avro.load("avro.properties");
    }

    public static Avro load(String filename) {
        Properties properties = Initialize.properties(filename);
        return new Avro(properties);
    }

    public S3Client getClient() {
        return client;
    }

    public String getReadBucket() {
        return readBucket;
    }

    public String getWriteBucket() {
        return writeBucket;
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

        String key = String.join("/", keyPrefix, UUID.randomUUID() + ".avro");
        Schema schema = records.get(0).getSchema();
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(records.get(0).getSchema());
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

        PipedInputStream inputStream = new PipedInputStream();
        PipedOutputStream outputStream = new PipedOutputStream(inputStream);

        dataFileWriter.create(schema, outputStream);
        for (GenericRecord record : records) dataFileWriter.append(record);
        dataFileWriter.close();

        client.putObject(
                PutObjectRequest.builder().bucket(writeBucket).key(key).build(),
                RequestBody.fromContentProvider(() -> inputStream, "application/octet-stream")
        );
    }
}
