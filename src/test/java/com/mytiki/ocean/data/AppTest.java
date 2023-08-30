/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.data;

import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.lambda.runtime.serialization.PojoSerializer;
import com.amazonaws.services.lambda.runtime.serialization.events.LambdaEventSerializers;
import com.amazonaws.services.lambda.runtime.tests.annotations.Event;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


@ExtendWith(MockitoExtension.class)
public class AppTest {

    AvroToParquet atp;

    @BeforeEach
    void init(@Mock AvroToParquet atp) throws IOException {
        this.atp = atp;
        Mockito.lenient().doReturn(List.of()).when(atp).read(Mockito.anyString(), Mockito.anyString());
        Mockito.lenient().doNothing().when(atp).write(Mockito.anyString(), Mockito.anyList());
        Mockito.lenient().doReturn("dummy").when(atp).notify(Mockito.any());
    }

    @ParameterizedTest
    @Event(value = "sqs-event.json", type = SQSEvent.class)
    public void event(SQSEvent event) {
        WriteHandler handler = new WriteHandler(atp);
        SQSBatchResponse rsp = handler.handleRequest(event, null);
        assertEquals(0, rsp.getBatchItemFailures().size());
    }

}
