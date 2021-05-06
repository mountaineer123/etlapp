# Build and Run Instructions

`run.cmd` // Windows
or
`run.sh` // Linux

3 directories will be created locally.

The following directories created are:

in - stores the downloaded zip(s) from S3
extracted - stores the cvs files with records that contain the search keyword(s), extracted
transformed - stores the parquet files generated from the cvs files, and the files uploaded to S3

Some code to generate parquet files where used from https://github.com/contactsunny/Parquet_File_Writer_POC

Author: Peter Wong
Date: 06 May 2021


