package com.nuix.awstest.s3;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public interface S3BucketClient {

    // Downloads an Object by Type from S3
    void downloadObjectByType(String type) throws IOException;

    void uploadObjectByType(String type);

    void setDownloadLocation(Path location);

    void setUploadLocation(Path location);

}
