import jdk.nashorn.internal.runtime.options.LoggingOption.LoggerInfo
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties
import scala.io.Source

object a12_DF_ExecutionPlan
{
def main(Args:Array[String]): Unit =
  {




    def Spark_Config_Func : SparkConf ={

    val SparkObj = new SparkConf();
      val props=new Properties()
 props.load(Source.fromFile("spark_configuration.conf").bufferedReader())
 props.forEach((k,v) => SparkObj.set(k.toString,v.toString))
 SparkObj

    }

    val spark=SparkSession.builder()
      .config(Spark_Config_Func)
      .getOrCreate()




    val df = spark.read
    .option("Header","true")
      .option("inferschema","true")
      .csv("D:\\Shobhita\\Spark\\Spark_input_data\\long_name.csv")



     val df2= df.repartition(2)
       .select("Age","Country")
     .where("Age>30")
    // .groupBy("Country")
      //.count()
    df.foreach(f => println(f.get(0)))
    df.show()


    while(true)
{

}
  }
}
