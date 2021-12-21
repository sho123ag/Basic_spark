import org.apache.spark.sql.SparkSession

object a19_Spark_Tables {
def main(Args:Array[String]): Unit =
  {
    val spark=SparkSession.builder()
      .appName("Spark_Managed_Tables")
      .master("local[3]")
      .enableHiveSupport()
      .getOrCreate()



    val df=spark.read
      .format("parquet")
      .option("path","D:\\Shobhita\\Spark\\Spark_input_data\\flight-time.parquet")
      .option("mode","FailFast")
      .load()
     print("Partitions of the DataFrame= "+df.rdd.getNumPartitions)
    spark.sql("Create Database if not exists Newdb")
   spark.catalog.listDatabases().show(false)
    df.write
      .format("json")
      .mode(saveMode = "Overwrite")
      .saveAsTable("Newdb.My_spark_Table") //It will create a table My_spark_Table in a default spark database name DEFAULT
                                                  // A new folder Spark-warehouse created locally
spark.catalog.listTables("Newdb").show()



  }
}
