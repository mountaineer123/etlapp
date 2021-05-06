package com.nuix.awstest.converter;


import com.nuix.awstest.converter.parquet.CustomParquetWriter;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import org.apache.hadoop.fs.Path;

import java.util.Arrays;
import java.util.List;

public class CSVParquetConverter implements Converter {

    private static final Logger logger = LoggerFactory.getLogger(CSVParquetConverter.class);

    public void convert(File sourceFile, File targetFile) throws IOException, CsvException {
        convertCsvToParquet(sourceFile, targetFile, false);
    }

    private void convertCsvToParquet(File sourceFile, File targetFile, boolean enableDictionary) throws IOException, CsvException {
        logger.info("Converting " + sourceFile.getName() + " to " + targetFile.getName());
        String rawSchema = this.getSchema(sourceFile);
        if(targetFile.exists()) {
            throw new IOException("Output file " + targetFile.getAbsolutePath() +
                    " already exists");
        }

        Path path = new Path(targetFile.toURI());

        MessageType schema = MessageTypeParser.parseMessageType(rawSchema);
        CustomParquetWriter writer = new CustomParquetWriter(path, schema, false, CompressionCodecName.SNAPPY);

        CSVReader csvReader = new CSVReader(new FileReader(sourceFile));
        List<String[]> entries = csvReader.readAll();

        try {
            for(String[] fields: entries) {
                writer.write(Arrays.asList(fields));
            }
            writer.close();
        } finally {
            this.closeResource(csvReader);
        }
    }

    private String getSchema(File csvFile) throws IOException, CsvException {
        String fileName = csvFile.getName().substring(0, csvFile.getName().length() - ".csv".length()) + ".schema";
        File schemaFile = new File(csvFile.getParentFile(), fileName);
        logger.info("Generating schema " + schemaFile);
        this.generateSchema(csvFile, schemaFile);
        return readFile(schemaFile.getAbsolutePath());
    }

    private void generateSchema(File sourceFile, File schemaFile) throws IOException, CsvException {
        BufferedReader br = new BufferedReader(new FileReader(sourceFile));
        PrintWriter schemaPW = new PrintWriter(schemaFile);
        CSVReader csvReader = new CSVReader(new FileReader(sourceFile));
        List<String[]> entries = csvReader.readAll();

        try {
            schemaPW.print("message schema { ");
            int i = 0;
            if(!entries.isEmpty()) {
                String[] fields = entries.get(0);
                for(String field: fields) {
                    schemaPW.print("optional binary column" + i++ + " (UTF8); ");
                };
            }
            schemaPW.print("}");
            schemaPW.flush();
        } finally {
            this.closeResource(csvReader);
            this.closeResource(schemaPW);
        }
    }

    private String readFile(String path) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(path));
        StringBuilder stringBuilder = new StringBuilder();

        try {
            String line = null;
            String ls = System.getProperty("line.separator");

            while ((line = reader.readLine()) != null ) {
                stringBuilder.append(line);
                stringBuilder.append(ls);
            }
        } finally {
            closeResource(reader);
        }

        return stringBuilder.toString();
    }

    private void closeResource(Closeable resource) {
        try {
            if(resource != null) {
                resource.close();
            }
        } catch (IOException ioe) {
            logger.warn("Exception closing resource " + resource + ": " + ioe.getMessage());
        }
    }
}
