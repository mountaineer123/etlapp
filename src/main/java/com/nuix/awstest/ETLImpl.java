package com.nuix.awstest;

import com.nuix.awstest.converter.Converter;
import com.nuix.awstest.s3.S3BucketClient;
import com.opencsv.exceptions.CsvException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.function.Predicate;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ETLImpl implements ETL {

    private static final Logger logger = LoggerFactory.getLogger(ETLImpl.class);

    private static final String IN_DIR = "in";
    private static final String EXTRACTED_DIR = "extracted";
    private static final String TRANSFORMED_DIR = "transformed";
    private static final String UPLOADED_DIR = "uploaded";

    private Path inPATH;
    private Path extractedPATH;
    private Path transformedPATH;
    private Path uploadedPATH;


    // Collaborators
    private S3BucketClient s3BucketClient;
    private Converter converter;

    public ETLImpl(S3BucketClient s3BucketClient, Converter converter) {
        try {
            this.inPATH = Files.createDirectories(Paths.get(System.getProperty("user.dir"), IN_DIR));
            this.extractedPATH = Files.createDirectories(Paths.get(System.getProperty("user.dir"), EXTRACTED_DIR));
            this.transformedPATH = Files.createDirectories(Paths.get(System.getProperty("user.dir"), TRANSFORMED_DIR));
            this.uploadedPATH = Files.createDirectories(Paths.get(System.getProperty("user.dir"), UPLOADED_DIR));

            this.s3BucketClient = s3BucketClient;
            this.s3BucketClient.setDownloadLocation(this.inPATH);
            this.s3BucketClient.setUploadLocation(this.transformedPATH);
            this.converter = converter;
            this.cleanUpFS();
        }
        catch(IOException e) {
            logger.error(e.getMessage());
        }
    }

    public Path getInPATH() {
        return inPATH;
    }
    public Path getExtractedPATH() { return extractedPATH; }
    public Path getTransformedPATH() { return transformedPATH; }
    public Path getUploadedPATH() { return uploadedPATH; }


    @Override
    public void downloadDataSet(String archiveType) throws IOException {
        logger.info("*** Downloading dataset ***");
        this.s3BucketClient.downloadObjectByType(archiveType);
    }

    // Unzips csv files and extracts as per filter
    @Override
    public void extractByFilter(Predicate<String> filter) throws IOException {
        logger.info("*** Performing Extraction ***");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(inPATH)) {
            for (Path file: stream) {
                logger.info("Unzipped file " + file.toString());
                extractFileByFilter(file, filter);
            }
        } catch (IOException | DirectoryIteratorException e) {
            logger.error(e.getMessage());
        }
    }

    private void extractFileByFilter(Path file, Predicate<String> filter) throws IOException {
        BufferedReader inputStream = null;
        PrintWriter outputStream = null;

        Path root = extractedPATH.normalize();
        try (InputStream is = Files.newInputStream(file); ZipInputStream zis = new ZipInputStream(is)) {
            ZipEntry entry = zis.getNextEntry();
            while (entry != null) {

                Path path = root.resolve(entry.getName()).normalize();
                if (!path.startsWith(root)) {
                    throw new IOException("Invalid ZIP");
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(path);
                } else {
                    logger.info("Unzipped file: " + path.toString());
                    inputStream = new BufferedReader(new InputStreamReader(zis));
                    outputStream = new PrintWriter(new OutputStreamWriter(Files.newOutputStream(path)));
                    String line;
                    while ((line = inputStream.readLine()) != null) {
                        if(filter.test(line)) {
                            outputStream.println(line);
                        }
                    }
                }
                outputStream.flush();
                entry = zis.getNextEntry();
            }
            zis.closeEntry();
        }
        finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (outputStream != null) {
                outputStream.close();
            }
        }
    }

    private void cleanUpFS() {
        logger.info("*** Cleaning up any previous ETLs ***");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(inPATH)) {
            for (Path file: stream) {
                logger.info("Deleting " + file.toString());
                Files.delete(file);
            }
        } catch (IOException | DirectoryIteratorException e) {
            logger.error(e.getMessage());
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(extractedPATH)) {
            for (Path file: stream) {
                logger.info("Deleting " + file.toString());
                Files.delete(file);
            }
        } catch (IOException | DirectoryIteratorException e) {
            logger.error(e.getMessage());
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(transformedPATH)) {
            for (Path file: stream) {
                logger.info("Deleting " + file.toString());
                Files.delete(file);
            }
        } catch (IOException | DirectoryIteratorException e) {
            logger.error(e.getMessage());
        }
    }

    public void transform() {
        logger.info("*** Performing Transforms ***");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(extractedPATH)) {
            for (Path streamItem: stream) {
                File extractedFile = new File(streamItem.toString());
                logger.info("Transforming " + extractedFile.toString());
                String filename = this.getFileNamePrefix(extractedFile);
                File transformedFile = this.getTargetFile(filename, true);
                this.converter.convert(extractedFile,transformedFile);
            }
        } catch (IOException | DirectoryIteratorException | CsvException e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void load(String type) {
        this.s3BucketClient.uploadObjectByType(type);
    }


    private String getFileNamePrefix(File file) {
        return file.getName().substring(0, file.getName().indexOf("."));
    }

    private File getTargetFile(String name, boolean deleteIfExists) {
        File outputFile = new File(transformedPATH.toString(), name + ".parquet");
        outputFile.getParentFile().mkdirs();
        if(deleteIfExists) {
            outputFile.delete();
        }
        return outputFile;
    }

}
