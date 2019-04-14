import org.apache.spark.{SparkConf, SparkContext} 
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object Habib_Boloorchi_Program_2 { 
	def main(args:Array[String]){
	val conf=new SparkConf().setAppName("Habib_Boloorchi_Program_2")//assign configurations
	val sc= SparkContext.getOrCreate()//create necessary context for application
	val spark =  SparkSession.builder().appName("Habib_Boloorchi_Program_2").getOrCreate()//create application
	
    
	
	val vehicles = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("hdfs://192.168.42.27:9000/CS5433/2019/vehicles.csv")//load dataset
	val makes= vehicles.select("make", "barrels08").cube("make").avg("barrels08")//select company of makers
    val orderedmakes= makes.orderBy(desc("avg(barrels08)")).show(5)//put average of barrels in order
	}
}