import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object a15_withcolumn {
  def main(Args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Dataset")
      .master("local[3]")
      .getOrCreate()


    val df = spark.read
      .option("Header", "true")
      .option("inferSchema", "true")
      .csv("D:\\Shobhita\\Spark\\Spark_input_data\\long_name.csv")
    df.printSchema()
    val df2=df.withColumn("Salary",col("Salary")+300).withColumn("Time",current_timestamp())
      .withColumn("size",length(col("Name")))
    df2.show(false)
  }
}
