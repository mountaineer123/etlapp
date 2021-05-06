package com.nuix.awstest.s3;

import com.nuix.awstest.ETL;
import com.nuix.awstest.ETLImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.nio.file.*;
import java.util.List;
import java.util.stream.Stream;

public class S3BucketClientImpl implements S3BucketClient {

    private static final Logger logger = LoggerFactory.getLogger(S3BucketClientImpl.class);

    private S3Client s3;
    private Region region;
    private String bucketName;
    private Path downloadLocation;
    private Path uploadLocation;

    public Path getDownloadLocation() {
        return downloadLocation;
    }

    public void setDownloadLocation(Path downloadLocation) {
        this.downloadLocation = downloadLocation;
    }

    public Path getUploadLocation() {
        return uploadLocation;
    }

    public void setUploadLocation(Path uploadLocation) {
        this.uploadLocation = uploadLocation;
    }

    public S3BucketClientImpl(Region region, String bucketName) {
        this.region = region;
        this.bucketName = bucketName;
    }

    private S3Client connect() {
        return S3Client.builder().region(region).build();
    }

    // Downloads an Object by Type
    //  1. List Objects in S3 bucket
    //  2. Find Object by Type
    //  3. Download Found Object
    @Override
    public void downloadObjectByType(String type) throws IOException {
        this.s3 = connect();
        List<S3Object> objects = listBucketObjects();
        logger.info("Found " + objects.size() + " objects");
        Stream<S3Object> objstream = objects.stream().filter(obj -> obj.key().endsWith(type));
        objstream.forEach(this::saveObjectToFile);
        this.s3.close();
    }

    @Override
    public void uploadObjectByType(String type) {
        this.s3 = connect();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(uploadLocation)) {
            for (Path file: stream) {
                this.logger.info("file to upload = " + file.getFileName().toString());

                String key = file.getFileName().toString();
                s3.putObject(PutObjectRequest.builder().bucket(this.bucketName).key(key).build(), file);

            }
        } catch (IOException | DirectoryIteratorException e) {
            logger.error(e.getMessage());
        }
        this.s3.close();
    }

    private void saveObjectToFile(S3Object obj) {
        String keyName = obj.key();
        logger.info("Found zip file name = " + keyName);
        Path zipFile = Paths.get(this.downloadLocation.toString(), keyName);
        this.s3.getObject(
                GetObjectRequest.builder().bucket(this.bucketName).key(keyName).build(),
                ResponseTransformer.toFile(zipFile));
        logger.info("Saved to " + zipFile.toString());
    }

    private List<S3Object> listBucketObjects() {
        try {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(this.bucketName)
                    .build();

            ListObjectsResponse res = s3.listObjects(listObjects);
            List<S3Object> objects = res.contents();

            return objects;

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }



}
