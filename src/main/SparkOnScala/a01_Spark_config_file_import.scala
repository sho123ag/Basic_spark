
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties
import scala.io.Source
object a01_Spark_config_file_import {
def main(Args:Array[String]): Unit =
  {


    def sparkConfFunc: SparkConf = {
      val SparkObj=new SparkConf();
      val props=new Properties();
      props.load(Source.fromFile("spark_configuration.conf").bufferedReader())
      props.forEach((k,v)=>SparkObj.set(k.toString,v.toString))
      SparkObj

    }

    val ses=SparkSession.builder()
      .config(sparkConfFunc)
      .getOrCreate()
  }
}
