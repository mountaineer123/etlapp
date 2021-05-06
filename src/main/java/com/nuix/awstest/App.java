package com.nuix.awstest;

import com.nuix.awstest.converter.CSVParquetConverter;
import com.nuix.awstest.s3.S3BucketClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;


public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    private static final String ARCHIVE_TYPE = "zip";
    private static final String SOURCE_TYPE = "csv";
    private static final String TARGET_TYPE = "parquet";

    private static final Region REGION = Region.AP_SOUTHEAST_2;

    public static void main(String[] args) throws IOException {

        String bucketName = args[0];
        logger.info("bucketName " + bucketName);
        String searchString = args[1];
        logger.info("searchString " + searchString);

        // Initializes ETL engine, injecting with S3BucketClient and CSVParquetConverter
        ETL etlEngine = new ETLImpl(new S3BucketClientImpl(REGION, bucketName), new CSVParquetConverter());

        // Downloads an Archive Object by Type (eg. zip file) from S3
        etlEngine.downloadDataSet(ARCHIVE_TYPE);

        // Extracts every File Object in Archive by Filter
        etlEngine.extractByFilter(l -> l.contains(searchString));

        // Transforms Extracted as per injected Converter
        etlEngine.transform();


        // 6. Upload Parquet files to S3 Bucket
        // 6a. List parquet files under folder
        // 6b. Upload file found
        //File parquetFile = new File();
        //s3BucketClient.uploadObject(s3, parquetFile);
        //etlEngine.load();
        etlEngine.load(TARGET_TYPE);

    }


}
