package fr.esme.tp
import fr.esme.tp.configuration.ConfigReader
import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{input_file_name, _}
import org.apache.spark.sql.{SaveMode, SparkSession}
import spray.json.DefaultJsonProtocol._
import spray.json._



object Launcher{

  def udf_test = udf((name: String, value:String) => {
    value
  })

  def main(args: Array[String]): Unit = {
    //  withConfig = true POUR prendre en compte le json Config
    //  withConfig = false POUR ne pas prendre en compte le json Config

    val withConfig = true


    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    // Q1. PART 1
    val df = sparkSession.read.option("header",false).option("inferSchema", true).option("delimiter", ",").csv("dataDate")
    df.show
    val columsWithoutSpaces = Seq("amount","base_currency","currency","exchange_rate")
    val df2 = df.toDF(columsWithoutSpaces:_*)
    df2.show



    // Q2. PART 1
    sparkSession.udf.register("get_file_name", (path: String) => path.split("/").last.split("\\.").head)
    val df3 =df2.withColumn("date", callUDF("get_file_name", input_file_name()))
    df3.show
    df3.orderBy("date").show


    // Q1 PART 2
    case class Conf(conf:Seq[ConfigDate])
    case class ConfigDate(date: String, fillWithDaysAgo:Int)
    implicit val confDateJsonFormat = jsonFormat2(ConfigDate)
    implicit val confJsonFormat = jsonFormat1(Conf)

    /*
    val confReader = ConfigReader.readConfig("conf/config.json")
    val configuration = confReader.parseJson.convertTo[Conf]
    println(configuration.conf)
    println(configuration.conf(0).fillWithDaysAgo)
    */

    val confRead = ConfigReader.readConfig("conf/configSample.json")
    val config = confRead.parseJson.convertTo[ConfigDate]
    val dateConfig = config.date
    val fillWithDaysAgoConfig = config.fillWithDaysAgo
    val expressionConfig = "INTERVAL "+fillWithDaysAgoConfig.toString+ " days"
    println(expressionConfig)

    //Q3 PART 1
    import org.apache.spark.sql.functions._
    var df5 = df3.withColumn("date1",
      to_timestamp(col("date"),"ddMMyyyy")).orderBy(asc("date1"))
    df5.show(50)
    val df_count3 = df5.groupBy("date1").count().orderBy("date1")


    var outputDir = ""
    var df_count4 = df_count3
    if(withConfig) {
      df_count4 = df_count3.withColumn("date", date_format(col("date1"), "ddMMyyyy"))
        .withColumn("date_veille", when(col("date") === dateConfig, col("date1") - expr(expressionConfig))
          .otherwise(col("date1") - expr("INTERVAL 1 days")))
      println("AVANT REMPLISSAGE DES GAPS")
      df_count4.show
      outputDir = "outputWithConfig"
    }else {
      df_count4 = df_count3.withColumn("date", date_format(col("date1"), "ddMMyyyy"))
        .withColumn("date_veille", col("date1") - expr("INTERVAL 1 days"))
      df_count4.show
      outputDir = "outputWithoutConfig"
    }

    val veilleDate2 = df_count4.select("date_veille").collect()
    val countDate2 = df_count4.select("count").collect()
    val distDate2 = df_count4.select("date1").collect()
    var j = 0
    for( d <- distDate2) {
      val veille = veilleDate2(j)(0)
      val count = countDate2(j)(0)
      if (count.toString.toInt < 8) {
        print("Date" + d(0) + " INCOMPLET")
        val df_to_add = df5.filter(col("date1")===veille)
          .limit(8-count.toString.toInt)
        val df_to_add2 = df_to_add.withColumn("date1", udf_test(col("date1"), lit(d(0))))
                  .withColumn("date", date_format(col("date1"),"ddMMyyyy"))
        df5 = df5.union(df_to_add2)
        println("APRES REMPLISSAGE DES GAPS")
        df5.groupBy("date").count().show()
      }
      j = j + 1
    }

    //Q4 PART 1     |    Q2 PART2
    df5 = df5.drop("date1")
    df5.write.mode(SaveMode.Overwrite).partitionBy("date").csv(outputDir)




  }

  }