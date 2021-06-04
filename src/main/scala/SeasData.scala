import Convert.{extractSeas, schema}
import org.apache.spark.sql.expressions.Window.orderBy
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pubsub.SparkPubsubMessage

object SeasData {

  private def findMaxSeasByElevation(input: DataFrame) =
    input
      .groupBy("elevation")
      .agg(
        row_number().over(orderBy(desc("elevation"))).as("Rank"),
        count("*").as("Count_Seas"),
        first("TimeStamp").as("TimeStamp")
      )
      .filter("Rank <= 1")

  private def findMinSeasByElevation(input: DataFrame) =
    input
      .where("elevation != 0")
      .groupBy("elevation")
      .agg(
        row_number().over(orderBy(asc("elevation"))).as("Rank"),
        count("*").as("Count_Seas"),
        first("TimeStamp").as("TimeStamp")
      )
      .filter("Rank <= 1")

  private def findMeanSeasByElevation(input: DataFrame) =
    input.select(avg(input("elevation"))).show()

  private def findCountSeasByElevation(input: DataFrame) =
    input
      .groupBy("elevation")
      .agg(
        count("*").as("Count_Seas"),
        first("TimeStamp").as("TimeStamp")
      )

  private def writeToBigquery(data: DataFrame, datasetName: String, tableName: String): Unit =
    data.write.format("bigquery").option("table", f"$datasetName.$tableName")
      .option("temporaryGcsBucket", "temp_bucket_for_table").mode(SaveMode.Append).save()

  def processSeasData(stream: DStream[SparkPubsubMessage], windowInterval: Int, slidingInterval: Int,
                      spark: SparkSession, bigQueryDataset: String): Unit = {
    stream.window(Seconds(windowInterval), Seconds(slidingInterval))
      .foreachRDD {
        rdd =>
          val seaDF = spark.createDataFrame(extractSeas(rdd), schema)
            .withColumn("TimeStamp", lit(date_format(current_timestamp(), "dd.MM.yyyy_hh-mm")))
            .cache()

          writeToBigquery(findMaxSeasByElevation(seaDF), bigQueryDataset, "output")
          writeToBigquery(findMinSeasByElevation(seaDF), bigQueryDataset, "output")
          writeToBigquery(findCountSeasByElevation(seaDF), bigQueryDataset, "count")
          findMeanSeasByElevation(seaDF)

          seaDF.write.mode(SaveMode.Overwrite).partitionBy("timestamp")
            .parquet("gs://bucket_for_parquet/app_output")
      }
  }
}