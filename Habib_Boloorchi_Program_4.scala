import org.apache.spark.{SparkConf, SparkContext} 
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming._
object Habib_Boloorchi_Program_4 { 
	def main(args:Array[String]){
	val conf=new SparkConf().setAppName("Habib_Boloorchi_Program_4")//assign configurations
	val sc= SparkContext.getOrCreate()//create necessary context for application
	val spark =  SparkSession.builder().appName("Habib_Boloorchi_Program_4").getOrCreate()//create application
	
    val static =  spark.read.json("hdfs://hadoop1:9000/hboloor/sparkflum")//it get whatever in flum is to variable
	val dataschema = static.schema//it findout the schema
	val streaming= spark.readStream.schema(dataschema).option("maxFilesPerTrigger",5).json("hdfs://hadoop1:9000/hboloor/sparkflum") // get 5 streaming from what flum got
	val tweets= streaming.select("text")//extract the text of tweets
	val spl = tweets.withColumn("words",explode(split(col("text")," "))).drop("text")//split the tweets to words andd put them in different rows
    val wordcount =  spl.groupBy("words").count()//find the frequency of whole words	
	val keys = Seq("Trump","Sanders","wall")//create the list words that we want to choose
	val result = wordcount.filter((col("words").isin(keys: _*)))//extractthe frequency of words that we chose
	val activityQuery = result.writeStream.queryName("activity_count").format("console").outputMode("complete").start()//begin to get data from stream and show it
	activityQuery.awaitTermination(10800000)///wait for 3 hours
	activityQuery.stop//stop activity
	}
}