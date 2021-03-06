# Build Instructions  

`mvn clean package`

# Run Instructions

`mvn exec:java -Dexec.mainClass="com.nuix.awstest.App" -Dexec.args="$S3BUCKET $KEYWORD"`  

where  
$S3BUCKET is the S3 bucket name (candidate-20-s3-bucket)  
$KEYWORD is the keyword used to extract records from the CSV files (ellipsis)  

### Alternatively

Run the following on Windows  
`run.cmd` 

Run the following on Linux  
`run.sh`

### Additional Notes

3 directories will be created locally.  

The following directories created are:  

in - stores the downloaded zip(s) from S3  
extracted - stores the cvs files with records that contain the search keyword(s), extracted  
transformed - stores the parquet files generated from the cvs files, and the files uploaded to S3  

Some code to generate parquet files where used from https://github.com/contactsunny/Parquet_File_Writer_POC  


Author: Peter Wong    
Date: 06 May 2021


