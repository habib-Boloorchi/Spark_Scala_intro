import org.apache.spark.{SparkConf, SparkContext} 
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object Habib_Boloorchi_Program_3 { 
	def main(args:Array[String]){
	val conf=new SparkConf().setAppName("Habib_Boloorchi_Program_3")//assign configurations
	val sc= SparkContext.getOrCreate()//create necessary context for application
	val spark =  SparkSession.builder().appName("Habib_Boloorchi_Program_3").getOrCreate()//create application
	
    
	
	val vehicles = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("hdfs://192.168.42.27:9000/CS5433/2019/vehicles.csv").createOrReplaceTempView("some_sql_view")//load dataset
	val makesnum = spark.sql("""SELECT fuelType1, COUNT(DISTINCT make) FROM some_sql_view GROUP BY fuelType1 """).show(5) //grupby fueltype of distinct value
	
	}
}