/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.data;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.apache.log4j.Logger;

import java.util.List;


public class App implements RequestHandler<SQSEvent, SQSBatchResponse> {
    protected static final Logger logger = Logger.getLogger(App.class);

    public SQSBatchResponse handleRequest(final SQSEvent event, final Context context) {
        try {
            Initialize.logger();
            WriteHandler handler = new WriteHandler(Avro.load());
            return handler.handleRequest(event, context);
        }catch (Exception ex){
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
