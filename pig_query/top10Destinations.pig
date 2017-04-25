queryData = LOAD '/home/neshant/dataset/dataset_files/pig_input.csv' USING PigStorage(',') AS (tripduration:int,starttime:chararray,stoptime:chararray,startstationid:int,startstationname:chararray,startstationlatitude:chararray,startstationlongitude:chararray,endstationid:int,endstationname:chararray,endstationlatitude:chararray,endstationlongitude:chararray,bikeid:int,usertype:chararray,birthyear:chararray,gender:int);
filterData = FILTER queryData BY (NOT (gender == 1));
groupNewData = GROUP filterData BY endstationname;
countData = FOREACH groupNewData GENERATE group, COUNT(filterData.endstationname) AS topDestination;
orderedData = ORDER countData by topDestination DESC;
finalData = LIMIT orderedData 10;
STORE finalData INTO 'outFile';

