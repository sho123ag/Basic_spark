import org.apache.spark.sql.{Row, SparkSession}

object a14_Dataset
{
  //case class recode( Age:Integer,Name:String,Salary:Integer)
  case class abc( Age:Integer,Name:String,Salary:Integer,County:String)
  def main(Args:Array[String]): Unit =
  {
    val spark=SparkSession.builder()
      .appName("Dataset")
      .master("local[3]")
      .getOrCreate()



    val df=spark.read
     .option("Header","true")
      //.option("inferSchema","true")
      .csv("D:\\Shobhita\\Spark\\Spark_input_data\\long_name.csv")
    df.printSchema()
    import spark.implicits._
    val df2=df.
      //select("Name","Salary","Age").as[recode]
      //
     map(r=>abc((Integer.parseInt(r.getString(1))+2),(r.getString(0).toUpperCase()),(Integer.parseInt(r.getString(2))+200),(r.getString(3).toLowerCase())))

    df2.show()


  }
}
