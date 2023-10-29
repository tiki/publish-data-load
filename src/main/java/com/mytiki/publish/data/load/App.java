/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.publish.data.load;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.lambda.runtime.serialization.PojoSerializer;
import com.amazonaws.services.lambda.runtime.serialization.events.LambdaEventSerializers;
import com.mytiki.publish.data.load.file.FileClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.util.List;


public class App implements RequestHandler<SQSEvent, SQSBatchResponse> {
    protected static final Logger logger = LogManager.getLogger(App.class);
    private final PojoSerializer<S3EventNotification> serializer =
            LambdaEventSerializers.serializerFor(S3EventNotification.class, ClassLoader.getSystemClassLoader());
    private final FileClient client = FileClient.load();

    public SQSBatchResponse handleRequest(final SQSEvent event, final Context context) {
        try {
            Handler handler = new Handler(client, serializer);
            return handler.handleRequest(event, context);
        } catch (Exception ex) {
            logger.error(ex, ex.fillInStackTrace());
            List<SQSBatchResponse.BatchItemFailure> all = event.getRecords().stream()
                    .map(ev -> new SQSBatchResponse.BatchItemFailure(ev.getMessageId()))
                    .toList();
            return SQSBatchResponse.builder()
                    .withBatchItemFailures(all)
                    .build();
        }
    }
}
