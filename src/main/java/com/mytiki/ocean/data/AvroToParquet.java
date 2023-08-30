/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.data;

import com.mytiki.ocean.common.Initialize;
import com.mytiki.ocean.common.Mapper;
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
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class AvroToParquet {
    private final S3Client s3;
    private final SqsClient sqs;
    private final String bucket;
    private final String credentialsProvider;
    private final String queue;

    public AvroToParquet(Properties properties) {
        sqs =  SqsClient.builder()
                .region(Region.of(properties.getProperty("s3.region")))
                .build();
        s3 = S3Client.builder()
                .region(Region.of(properties.getProperty("sqs.region")))
                .build();
        bucket = properties.getProperty("s3.bucket");
        credentialsProvider = properties.getProperty("s3.credentials.provider");
        queue = properties.getProperty("sqs.url");
    }

    public static AvroToParquet load() {
        return AvroToParquet.load("aws.properties");
    }

    public static AvroToParquet load(String filename) {
        Properties properties = Initialize.properties(filename);
        return new AvroToParquet(properties);
    }

    public List<GenericRecord> read(String bucket, String key) throws IOException {
        List<GenericRecord> records = new ArrayList<>();
        ResponseInputStream<GetObjectResponse> inputStream = s3.getObject(
                GetObjectRequest.builder().bucket(bucket).key(key).build(),
                ResponseTransformer.toInputStream()
        );

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileStream<GenericRecord> fileStream = new DataFileStream<>(inputStream, datumReader);
        while (fileStream.hasNext()) records.add(fileStream.next());

        return records;
    }

    public FileMetadata write(String table, List<GenericRecord> records) throws IOException {
        Schema schema = records.get(0).getSchema();
        String key = String.join("/", table, UUID.randomUUID() + ".parquet");
        Configuration conf = new Configuration();
        conf.set("fs.s3a.aws.credentials.provider", credentialsProvider);
        ParquetWriter<Object> writer = AvroParquetWriter
                .builder(new Path(String.join("/", "s3a:/", bucket, key)))
                .withSchema(schema)
                .withConf(conf)
                .build();

        for (GenericRecord record : records) writer.write(record);
        writer.close();

        GetObjectAttributesRequest attributesRequest = GetObjectAttributesRequest.builder()
                .bucket(bucket)
                .key(key)
                .objectAttributes(ObjectAttributes.OBJECT_SIZE)
                .build();

        GetObjectAttributesResponse rsp = s3.getObjectAttributes(attributesRequest);
        FileMetadata writeDetails = new FileMetadata();
        writeDetails.setSize(rsp.objectSize());
        writeDetails.setUri(String.join("/", "s3:/", bucket, key));
        writeDetails.setCount(records.size());
        writeDetails.setTable(table);
        return writeDetails;
    }

    public String notify(FileMetadata details){
        SendMessageRequest request = SendMessageRequest.builder()
                .queueUrl(queue)
                .messageBody(new Mapper().writeValueAsString(details))
                .messageGroupId(details.getTable())
                .build();
        SendMessageResponse rsp = sqs.sendMessage(request);
        return rsp.messageId();
    }
}
