# NYC Taxi Trip Hot Spots

This program identified spatio-temporal hot spots in New York City Yellow Cab taxi trips using Getis-Ord Gi* statistic.
* __DataSet:__  New York City Yellow Cab taxi trip records from January 2009 to June 2015
http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
* __Compile Dependencies:__ Information about compile dependencies is provided in the simple.sbt file.

## Run Command
```
spark-submit   --jars joda-convert-1.7.jar --jars joda-time-2.9.jar --class "HotSpots"  --driver-memory 8g --master local[1] target/scala-2.10/*_2.10-1.0.jar input_test output 0.001 2
```

## Q&A

1.  Where is the source code?
Source code is located in [src/main/scala/hotSpot.scala](https://github.com/yiic/NYC-Hot-Spots/blob/master/src/main/scala/HotSpot.scala)
2. What is the format of input file?
A sample input file is in [input_test/test_verify.csv](https://github.com/yiic/NYC-Hot-Spots/blob/master/input_test/test_verify.csv)

	
