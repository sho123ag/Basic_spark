

import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object a20_Working_With_DataframeRows
{
def main(Args:Array[String]): Unit = {
  val spark = SparkSession.builder()
    .appName("DtaFrame_Rows")
    .master("local[3]")
    .getOrCreate()

val my_sch=StructType(List(
  StructField("Name",StringType),
  StructField("Age",IntegerType),
  StructField("DOB",StringType),
  StructField("Sal",IntegerType)
))

  val myRows=List(
    Row("Shobhita",22,"24/01/1997",333),
    Row("John",66,"02/11/1952",444),
    Row("Rachel",51,"05/05/1957",555),
    Row("Mike",55,"24/01/1997",666),
      Row("Smith",22,"24/03/1997",777)
  )

val rdd=spark.sparkContext.parallelize(myRows)
  val df=spark.createDataFrame(rdd,my_sch)

 /* val df = spark.read
    .format("csv")
    .option("header","true")
    .option("path", "D:\\Shobhita\\Spark\\Spark_input_data\\long_name.csv")
    .load()*/
  df.printSchema()
  df.show()

  def convert(df:DataFrame,fmt:String): DataFrame={
    df.withColumn("DOB",to_date(col("DOB"),fmt))
  }
  val converted_df=convert(df,"dd/MM/yyyy")
  converted_df.printSchema()
  converted_df.show()







}

}


