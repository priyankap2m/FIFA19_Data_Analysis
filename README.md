# FIFA19_Data_Analysis
Implement a solution to store the FIFA19 dataset to enable frequent query and efficient
analysis. Assume that the dataset size as well as the frequency of load may increase in the
future as FIFA’s daily delta datasets are available. Explain what you considered and the
reason for your choice. 

# Solution: 
Change Data Capture  using Spark

This pattern is fundamentally based upon calculating a deterministic hash of the key and non-key attribute(s), and then using this hash as the basis for comparison. The hashes are stored with each record in perpetuity as the pattern is premised upon immutable data structures (such as HDFS, S3, GCS, etc). The pattern provides distillation and reconstitution of data during the process to minimize memory overhead and shuffle I/O for distributed systems, as well as breaking the pattern into discrete stages (designed to minimize the impact to other applications). This pattern can be used to process delta or full datasets.

A high-level flowchart representing the basic pattern is shown here:

Usage
Create Test Input Datasets:
spark-submit synthetic-cdc-data-generator.py 10000 10000 5 10 0.2 0.4 0.4 data/day1 data/day2
or specify your desired number of records for day 1 and day 2, key fields and non key fields (see LINK)

Day 1:
spark-submit cdc.py config.yaml data/FIFA19_Day_1.csv 2019-06-18
Day 2:
spark-submit cdc.py config.yaml data/FIFA19_Day_2.csv 2019-06-19
The Results
The output from processing the Day 1 file should be:

ingesting 10000 new records...
Writing out all incoming records as INSERT to current object (file:///tmp/sample_data_current)...
Finished writing out INSERT records to current object (0:00:02.630586)
Writing out INSERT records to history object (file:///tmp/sample_data_history/2019-06-18)...
Finished writing out INSERT records to history object (0:00:02.205599)
Finished CDC Processing!
The output from processing the Day 2 file should be:

ingesting 10000 new records...
Writing out INSERT records to current object (file:///tmp/sample_data_current_temp)...
Finished writing out INSERT records to current object (0:00:39.105516)
Writing out INSERT records to history object (file:///tmp/sample_data_history/2019-06-19)...
Finished writing out INSERT records to history object (0:00:05.191047)
Writing out DELETE records to history object (file:///tmp/sample_data_history/2019-06-19)...
Finished writing out DELETE records to history object (0:00:00.897985)
Writing out UPDATE records to current object (file:///tmp/sample_data_current_temp)...
Finished writing out UPDATE records to current object (0:00:05.292828)
Writing out UPDATE records to history object (file:///tmp/sample_data_history/2019-06-19)...
Finished writing out UPDATE records to history object (0:00:04.691484)
Writing out NOCHANGE records to current object (file:///tmp/sample_data_current_temp)...
Finished writing out NOCHANGE records to current object (0:00:01.010659)
Finished CDC Processing!
Inspection of the resultant current state of the players object using:

df = spark.read.parquet("file:///tmp/sample_data_current_temp")
df.groupBy(df.operation).count().show()
Produces the following output:

+---------+-----+
|operation|count|
+---------+-----+
|        U| 4000|
|        N| 4000|
|        I| 2000|
+---------+-----+

# Determine UPDATEs or Unchanged Records
Again, referring to the previous full outer join, keys which exist in both the incoming and current datasets must be either the result of an UPDATE or they could be unchanged. To determine which case they fall under, compare the non key hashes. If the non key hashes differ, it must have been a result of an UPDATE operation at the source, otherwise the record would be unchanged.

Tag these records as U or N respectively with an EFF_START_DATE of the business effective date (in the case of an update - otherwise maintain the current EFF_START_DATE), rejoin these records with their full attribute payload from the incoming dataset, then write out these records to the current and historical partition in append mode.

Key Pattern Callouts
A summary of the key callouts from this pattern are:

Use the RDD API for iterative record operations (such as type casting and hashing)
Persist hashes with the records
Use Dataframes for JOIN operations
Only perform JOINs with the keyhash and nonkeyhash columns – this minimizes the amount of data shuffled across the network
Write output data in columnar (Parquet) format
Break the routine into stages, covering each operation, culminating with a saveAsParquet() action – this may seem expensive but for large datsets it is more efficient to break down DAGs for each operation
Use caching for objects which will be reused between actions
