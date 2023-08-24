/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.data;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.junit.Test;

import java.util.List;

public class AppTest {

    @Test
    public void success() {
        App app = new App();
        SQSEvent.SQSMessage message = new SQSEvent.SQSMessage();
        message.setBody("hello");
        List<SQSEvent.SQSMessage> messages = List.of(message);
        SQSEvent event = new SQSEvent();
        event.setRecords(messages);
        app.handleRequest(event, null);
    }
}
