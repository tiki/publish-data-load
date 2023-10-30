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
import com.mytiki.publish.data.load.file.FileClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Handler implements RequestHandler<SQSEvent, SQSBatchResponse> {
    protected static final Logger logger = LogManager.getLogger(Handler.class);
    private final FileClient client;
    private final PojoSerializer<S3EventNotification> serializer;

    public Handler(FileClient client, PojoSerializer<S3EventNotification> serializer) {
        this.client = client;
        this.serializer = serializer;
    }

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        List<SQSBatchResponse.BatchItemFailure> failures = new ArrayList<>();
        Map<String, List<GenericRecord>> tableGrouping = new HashMap<>();

        event.getRecords().forEach(ev -> {
            S3EventNotification s3Event = serializer.fromJson(ev.getBody());
            s3Event.getRecords().forEach(record -> {
                try {
                    String key = record.getS3().getObject().getKey();
                    String table = key.split("/")[0];
                    if (table == null) throw new IllegalArgumentException("No table name in key: " + key);
                    List<GenericRecord> stagedRecords = client.read(key);
                    if (tableGrouping.containsKey(table)) tableGrouping.get(table).addAll(stagedRecords);
                    else tableGrouping.put(table, stagedRecords);
                } catch (Exception ex) {
                    logger.error(ex, ex.fillInStackTrace());
                    failures.add(new SQSBatchResponse.BatchItemFailure(ev.getMessageId()));
                }
            });
        });

        for (Map.Entry<String, List<GenericRecord>> entry : tableGrouping.entrySet()) {
            String table = entry.getKey();
            List<GenericRecord> records = entry.getValue();
            if (records.isEmpty()) {
                logger.warn("No records. Skipping. Table: " + table);
            } else {
                try {
                    client.write(table, records);
                } catch (Exception ex) {
                    logger.error(ex, ex.fillInStackTrace());
                    throw new RuntimeException(ex);
                }
            }
        }

        return SQSBatchResponse.builder()
                .withBatchItemFailures(failures)
                .build();
    }
}
