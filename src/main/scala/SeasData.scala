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
        row_number().over(orderBy(count("*").desc)).as("Rank"),
        count("*").as("Count_Seas"),
        first("TimeStamp").as("TimeStamp")
      )
      .filter("Rank <= 3")

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

          writeToBigquery(findMaxSeasByElevation(seaDF), bigQueryDataset, "max")

          seaDF.write.mode(SaveMode.Append).partitionBy("TimeStamp")
            .parquet("alien-vim-314816:test_output.table_temp")
      }
  }
}