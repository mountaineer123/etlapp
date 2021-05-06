package com.nuix.awstest;

import java.io.IOException;
import java.util.function.Predicate;

public interface ETL {

    // Downloads a DataSet by Type (eg. zip file) from S3
    void downloadDataSet(String type) throws IOException;

    // Filters every File Object in Archive by Criteria
    void extractByFilter(Predicate<String> criteria) throws IOException;

    // Transforms every File Object in Archive using Collaborator Converter
    void transform();

    // (Up)Loads Transformed (parquet) files to S3
    void load(String type);
}
