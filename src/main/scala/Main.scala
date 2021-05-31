import SeasData.processSeasData
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main {
  def createContext(projectID: String, bigQueryDataset: String, windowInterval: Int, slidingInterval: Int): StreamingContext = {
    val appName = "SparkSeas"
    val conf = new SparkConf().setMaster("local[2]").setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(slidingInterval))

    val spark: SparkSession = SparkSession
      .builder()
      .appName(appName)
      .config(conf)
      .getOrCreate()

    val crashesStream = PubsubUtils
      .createStream(
        ssc,
        projectID,
        None,
        "seas_reader",
        SparkGCPCredentials.builder.build(), StorageLevel.MEMORY_AND_DISK_SER_2)

    processSeasData(crashesStream, windowInterval, slidingInterval, spark, bigQueryDataset)
    ssc
  }

  def main(args: Array[String]) : Unit = {

    if (args.length != 5) {
      System.err.println(
        """
          |
          |     <projectID>: ID of Google Cloud project
          |     <bigQueryDataset>: The name of the bigquery dataset for saving metrics
          |     <windowLength>: The duration of the window, in seconds
          |     <slidingInterval>: The interval at which the window calculation is performed, in seconds.
          |     <totalRunningTime>: Total running time for the application, in seconds. If 0, runs indefinitely until termination.
          |
        """.stripMargin)
      System.exit(1)
    }

    val Seq(projectID, bigQueryDataset, windowInterval, slidingInterval, totalRunningTime) = args.toSeq

    val ssc = createContext(projectID, bigQueryDataset, windowInterval.toInt, slidingInterval.toInt)
    ssc.start()

    if (totalRunningTime.toInt == 0) {
      ssc.awaitTermination()
    }
    else {
      ssc.awaitTerminationOrTimeout(1000 * totalRunningTime.toInt)
    }
  }
}
