import org.apache.spark.sql.SparkSession
object ExecutorMain
{
def main(args:Array[String]): Unit =
  {
    val ss=SparkSession.
      builder()
      .appName(name = "Dusht Aditya")
      .master(master = "local[3]")
      .getOrCreate()
while(true)
  {

  }
  }
}
