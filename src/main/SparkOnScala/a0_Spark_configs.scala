import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object a0_Spark_configs {
  def main(Args:Array[String]): Unit =
  {

      //.appName(name= " Seeting configurations")
    //  .master(master= "local[n]")
    val Sparkobj=new SparkConf();
    Sparkobj.set("spark.app.name","Setting Configs")
    Sparkobj.set("spark.master","local[4]")
      val sess=SparkSession.builder()
      .config(Sparkobj)
      .getOrCreate()


  }

}
