import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.spark_partition_id

object a18DataFrame_Writer
{
def main(Args: Array[String]): Unit =
  {
    val spark=SparkSession.builder()
      .appName("DataFrame_Writer")
      .master("local[3]")
      .getOrCreate()



    val df=spark.read
      .format("parquet")
      .option("path","D:\\Shobhita\\Spark\\Spark_input_data\\flight-time.parquet")
      .load()

   val df2= df.repartition(5)
    //df.printSchema();
    //df.show(numRows = 10)

   df2.groupBy(spark_partition_id()).count().show()
    print("No of parttions"+ df2.rdd.getNumPartitions+"    ")
    /*df2.write
      .format("avro")       //No of avro file is equal to no. of partitions
      .option("path","D:\\Shobhita\\Spark\\Spark_input_data\\output")
      .mode(saveMode = "Overwrite")
      .save( )
*/
    df2.write.format("json")
      .option("path","D:\\Shobhita\\Spark\\Spark_input_data\\output")
      .mode(saveMode = "Overwrite")
      .partitionBy("OP_CARRIER","ORIGIN") ///Your data is partitioned by Unique combination of partition columns.
      //Partition by help in elimination of data partitions which are not required
      .save( )

  }
}
