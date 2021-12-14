import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
case class surveyRDD(Name:String,Age:Int,sal:Int,Country:String)
object a13_RDD {
def main(Args:Array[String]): Unit =
  {
   val spark_conf_Obj= new SparkConf()
     .set("spark.app.name","RDD INTRO")
     .set("spark.master","local[3]")
    val sparkCon=new SparkContext(spark_conf_Obj)
    val rdd1=sparkCon.textFile("D:\\Shobhita\\Spark\\Spark_input_data\\long_name.csv")

   val rdd2= rdd1.filter(k => !(k.split(",")(0).equals("Name")))
   val rdd3= rdd2.map(k => k.split(",")(0)+" "+(Integer.parseInt(k.split(",")(2))+200))
    rdd3.foreach(println)

val rdd4 = rdd1.map(l=> l.split(","))
rdd4.foreach(k=>println(k.mkString(",")))
    println("No Of Partitions")
println(rdd3.getNumPartitions)



  }
}
