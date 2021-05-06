package com.nuix.awstest.converter;

import com.opencsv.exceptions.CsvException;

import java.io.File;
import java.io.IOException;

public interface Converter {
    public void convert(File sourceFile, File targetFile) throws IOException, CsvException;
}
