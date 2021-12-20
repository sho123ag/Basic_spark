import org.apache.spark.sql.SparkSession

object a16_Using_Spark_Sql
{
  def main(Args:Array[String]): Unit =
  {
    val spark =SparkSession.builder()
      .appName("Using_Spark_Sql")
      .master("local[3]")
      .getOrCreate()

    val df=spark.read
      .option("Header","true")
      .csv("D:\\Shobhita\\Spark\\Spark_input_data\\long_name.csv")

    //We can use sql only on table and views.so we need to register the dataframe as view.

    df.createOrReplaceTempView("Something")
    val df_Name=spark.sql("Select Name from Something where Name like 'M%'")
    df_Name.show()

  }

}
