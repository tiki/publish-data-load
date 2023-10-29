/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.publish.data.load.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mytiki.publish.data.load.file.FileMetadata;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class AwsSQS {
    private final SqsClient sqs;
    private final String queue;
    private final ObjectMapper mapper = new ObjectMapper();

    public AwsSQS(String region, String queue) {
        sqs = SqsClient.builder()
                .region(Region.of(region))
                .build();
        this.queue = queue;
    }

    public String getQueue() {
        return queue;
    }

    public String notify(FileMetadata metadata) throws JsonProcessingException {
        SendMessageRequest request = SendMessageRequest.builder()
                .queueUrl(queue)
                .messageBody(mapper.writeValueAsString(metadata))
                .messageGroupId(metadata.getTable())
                .build();
        SendMessageResponse rsp = sqs.sendMessage(request);
        return rsp.messageId();
    }
}
