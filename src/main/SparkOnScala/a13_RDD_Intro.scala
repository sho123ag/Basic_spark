import org.apache.spark.{SparkConf, SparkContext}

object a13_RDD_Intro {

 case class SurveyRecord(Name:String,Salary:Int,Country:String)
  def main(Args: Array[String]): Unit =
  {
    val Spark_conf_obj=new SparkConf()         //Creating Spark Conf object to create a spark Context
      .set("spark.app.name","RDD_Creation")
      .set("spark.master","local[3]")

    val sc=new SparkContext(Spark_conf_obj)    // Pass Spark Conf object in the spark Context
    val rdd1=sc.textFile("D:\\Shobhita\\Spark\\Spark_input_data\\long_name.csv")  // read a file into rdd


    //TASK:Create a new rdd by increasing sal 200 to each and print it with name

    val rdd02=rdd1.filter(k=> !(k.split(",")(0).equals("Name"))) //filter out thee header
    val rdd2=rdd02.map(k => (k.split(",")(0))+","+(Integer.parseInt(k.split(",")(2))+200)+","+(k.split(",")(3))) //(it will be a RDDcolumns[STring] because " " added 2 )
    rdd2.foreach(k=>println(k))

    val rdd3 = rdd2.map(k=>
  {
    val cols= k.split(",")
    val rdd4=SurveyRecord(cols(0),cols(1).toInt,cols(2))
    rdd4
  })
    //rdd3.foreach(k=>println(k))

    val rdd5=rdd3.filter(k=>k.Salary>4000)
    rdd5.foreach(k=>println(k))




  }
}
