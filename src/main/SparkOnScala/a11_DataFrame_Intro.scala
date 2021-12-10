import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object a11_DataFrame_Intro
{
def main(Args:Array[String]): Unit =
  {
    val Spark_Conf_Obj= new SparkConf();
    Spark_Conf_Obj.set("spark.app.name","Dataframes")
    Spark_Conf_Obj.set("spark.master","local[3]")

    val spark= SparkSession.builder()
      .config(Spark_Conf_Obj)
      .getOrCreate()

    val df=spark.read
      .option("header",true)
      .csv("D:\\Shobhita\\Spark\\Spark_input_data\\long_name.csv")

    val df1 = df.where("Age>30")
      .select("Country","Name")
    //df1.show(20,false)

    val df2=df.groupBy("Country")
    val df3=df2.count()
    df3.show()
  }
}
