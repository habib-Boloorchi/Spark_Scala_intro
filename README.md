# Spark_Scala_intro
create build.sbt
 
create this folder : src/main/scala/

put the program and cd ../../..

run : sbt clean //clean last build and create target and .. folders

run: sbt package //it compiles)

to run the app :   spark-submit --class Habib_Boloorchi_Program_3 ./target/scala-2.10/habib_boloorchi_program_3_2.10-1.0.jar

Project Definition:

1.For all vehicles, write a Sparkjob to get the average annual petroleum consumption in barrels for fuelType1for each makeof vehicle.

2.Write a Spark job using the cubefunction to get the the average annual petroleum consumption in barrels for fuelType1 for each make of vehicle.

3.Write a Spark job using SQL to find the total number of makes of vehicles for all years that run on different types of fuelType1.

4.Stream twitter datainto Spark for 3hours. After 3 hours the streaming should automatically stop. Identify three topic keywords. As the data is streaming in, based on the 3 topic keywords, count the number of times the keywords appear in the tweets. Do this whenever a new file is input. Output the count for each keyword. Caution: Do not use common words.

5.Write a Spark trigger basedon the processing time. Execute the streaming query of No. 4above at regular 10 minute intervals.After 3 hours the streaming should automatically stop.

How to catch Twitter data in flume:

Create and sign in twitter, create configuration file 
I put a conf file example in the repositort. and run it by this command in HDFS:

&nohup $FLUME_HOME/bin/flume-ng agent -n TwitterAgent -f /autohome/file_name.conf &


