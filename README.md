# Spark_Scala_intro
create build.sbt
 
create this folder : src/main/scala/
put the program and cd ../../..

run : sbt clean //clean last build and create target and .. folders
run: sbt package //it compiles)
to run the app :   spark-submit --class Habib_Boloorchi_Program_3 ./target/scala-2.10/habib_boloorchi_program_3_2.10-1.0.jar
