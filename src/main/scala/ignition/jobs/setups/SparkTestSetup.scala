package ignition.jobs.setups

import ignition.jobs.WordCountJob

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType}

import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.core.jobs.utils.SparkContextUtils._

import ignition.core.jobs.utils.SimplePathDateExtractor.default

object SparkTestSetup {

    def run(runnerContext: RunnerContext) {

        val sc = runnerContext.sparkContext
        val sparkConfig = runnerContext.config

        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._

        println("######## Reading companies.json file")
        val df = sqlContext.read.json("/tmp/companies.json")
        
        println("######## Part 1.1 Result")
        item1(df)

        println("######## Part 1.2 Result")
        item2(df)

        println("######## Part 1.3 Result")
        item3(df)
    }

    def item1(df: DataFrame) {

        df.registerTempTable("company")

        val overviewLength = df.select(col("_id") as "id", col("name"), length(col("overview")) as "length")

        overviewLength.orderBy(desc("length")).show(10, false)

    }

    def item2(df: DataFrame) {

        val tags = df.select("tag_list")
        val taglistrdd = tags.rdd.map(r => r(0).asInstanceOf[String]).filter(x => x != null)
        taglistrdd.flatMap(x => x.split(", ")).filter(_.nonEmpty)
            .map((_, 1)).reduceByKey(_ + _).sortBy(x => x._2, false).take(10).foreach(println)

    }

    def item3(df: DataFrame) {
        val tags = df.select("founded_year")
            .filter(col("founded_year") >= 1994)
            .filter(col("founded_year") <= 2013)
        val taglistrdd = tags.rdd.map(r => r(0).toString).filter(x => x != null)
        taglistrdd.map((_, 1)).reduceByKey(_ + _).collect.sorted.foreach(println)
    }


    //this only works on Spark 2.2+
    // def item3(df: DataFrame, sqlContext: SQLContext) {
    //     val schema = StructType(Array(
    //         StructField("Name",StringType),
    //         StructField("Year",LongType)
    //     ))

    //     val selectedDf = df.select("name","founded_year").filter(col("founded_year") >= 1994)
    //         .filter(col("founded_year") <= 2013)

    //     val companyDf = spark.createDataFrame(selectedDf.rdd, schema)

    //     companyDf.filter(col("Year").isNotNull).map(value => value.getLong(1)).rdd.histogram(10)

    // }

}
