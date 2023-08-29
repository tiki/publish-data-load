/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.data;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.lambda.runtime.serialization.PojoSerializer;
import com.amazonaws.services.lambda.runtime.serialization.events.LambdaEventSerializers;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.junit.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

public class AppTest {

    //@Event(value = "sqs-event.json", type = SQSEvent.class)
    public void event(SQSEvent event) throws JsonProcessingException {
        final PojoSerializer<S3EventNotification> s3EventSerializer =
                LambdaEventSerializers.serializerFor(S3EventNotification.class, ClassLoader.getSystemClassLoader());

        S3EventNotification eventNotification = s3EventSerializer.fromJson(event.getRecords().get(0).getBody());
        assertNotNull(eventNotification);
    }

    @Test
    public void tester() throws IOException {
        AvroToParquet avro = AvroToParquet.load();
        List<GenericRecord> records = avro.read("test-data.avro");
        avro.write("output", records);
    }

    @Test
    public void testy() throws URISyntaxException {
        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource("all.avro");

        List<GenericRecord> all = new ArrayList<>();
        all.addAll(read(new File(resource.toURI())));

        assertEquals(2, all.size());
    }

    @Test
    public void success() throws URISyntaxException, IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        URL resource1 = classLoader.getResource("test-data-1.avro");
        URL resource2 = classLoader.getResource("test-data-2.avro");

        List<GenericRecord> all = new ArrayList<>();
        all.addAll(read(new File(resource1.toURI())));
        all.addAll(read(new File(resource2.toURI())));
        assertEquals(2, all.size());

        File file = new File("all.avro");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(all.get(0).getSchema());
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(all.get(0).getSchema(), file);
        all.forEach(record -> {
            try {
                dataFileWriter.append(record);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        dataFileWriter.close();
    }

    List<GenericRecord> read(File testData) {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(testData, datumReader)) {
            List<GenericRecord> records = new ArrayList<>();
            while (dataFileReader.hasNext()) {
                records.add(dataFileReader.next());
            }
            return records;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void read() throws URISyntaxException, IOException {
        S3Client s3 = S3Client.builder()
                //.crossRegionAccessEnabled(true)
                .region(Region.US_EAST_2)
                .build();


        ResponseInputStream<GetObjectResponse> s3Stream = s3.getObject(
                GetObjectRequest.builder().bucket("tiki-ocean-test").key("test-data.avro").build(),
                ResponseTransformer.toInputStream()
        );


        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileStream<GenericRecord> fileStream = new DataFileStream<>(s3Stream, datumReader);


//        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
//                new SeekableByteArrayInput(bytes.asByteArray()), datumReader);

        GenericRecord record = fileStream.next();
        Schema schema = record.getSchema();
        assertNotNull(schema);
        assertNotNull(record);
    }

}
