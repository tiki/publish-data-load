/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.publish.data.load.aws;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.util.Properties;

public class AwsS3 {
    private final S3Client s3;
    private final String bucket;

    public AwsS3(String region, String bucket) {
        s3 = S3Client.builder()
                .region(Region.of(region))
                .build();
        this.bucket = bucket;
    }

    public String getBucket() {
        return bucket;
    }

    public ResponseInputStream<GetObjectResponse> get(String key) {
        return s3.getObject(
                GetObjectRequest.builder().bucket(bucket).key(key).build(),
                ResponseTransformer.toInputStream()
        );
    }

    public Long size(String key) {
        GetObjectAttributesRequest attributesRequest = GetObjectAttributesRequest.builder()
                .bucket(bucket)
                .key(key)
                .objectAttributes(ObjectAttributes.OBJECT_SIZE)
                .build();
        GetObjectAttributesResponse rsp = s3.getObjectAttributes(attributesRequest);
        return rsp.objectSize();
    }
}
