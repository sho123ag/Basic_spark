import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, to_date}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
object a17_Explicit_Schema {

  def main(Args:Array[String]): Unit =
  {
    val my_sch=StructType(List(
      StructField("Name",StringType),
      StructField("Age",IntegerType),
      StructField("Sal",IntegerType),
      StructField("Country",StringType),
      StructField("date",DateType)          //Fail to parse '24-01-1997' in the new parser. (Only this line addition will give this error.
                                              //(Spark Dataframe reader is not able to parse date field as dateType.Need to define date format)
    ))

    val spark=SparkSession.builder()
      .appName("MY_Schema")
      .master("local[3]")
      .getOrCreate()

    val df=spark.read
      .format("csv")
      .option("header","true")
      .option("mode","Permissive")
      //.option("inferschema","true") //date: string (nullable = true) It is taking Date as string
      .option("path","D:\\Shobhita\\Spark\\Spark_input_data\\long_name.csv")
      .schema(my_sch)
      .option("dateFormat","dd/MM/yyyy")
      .load()
  //  val df1=df.withColumn("date",to_date(col("date"),"dd-MM-yyyy"))

    df.printSchema();
    df.show();



  }
}