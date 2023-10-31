/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.publish.data.load;

import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.lambda.runtime.serialization.PojoSerializer;
import com.amazonaws.services.lambda.runtime.serialization.events.LambdaEventSerializers;
import com.amazonaws.services.lambda.runtime.tests.annotations.Event;
import com.mytiki.publish.data.load.file.FileClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AppTest {

    FileClient client;
    final PojoSerializer<S3EventNotification> serializer =
            LambdaEventSerializers.serializerFor(S3EventNotification.class, ClassLoader.getSystemClassLoader());

    @BeforeEach
    void init() throws IOException {
        this.client = Mockito.mock(FileClient.class);
        Mockito.lenient().doReturn(List.of()).when(client).read(Mockito.anyString(), Mockito.anyString());
        Mockito.lenient().doReturn("dummy").when(client).write(Mockito.anyString(), Mockito.anyList());
    }

    @ParameterizedTest
    @Event(value = "sqs-event.json", type = SQSEvent.class)
    public void event(SQSEvent event) {
        Handler handler = new Handler(client, serializer);
        SQSBatchResponse rsp = handler.handleRequest(event, null);
        assertEquals(0, rsp.getBatchItemFailures().size());
    }

}
