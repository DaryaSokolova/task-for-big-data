import java.nio.charset.StandardCharsets
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.pubsub.SparkPubsubMessage

import scala.collection.JavaConverters._

object Convert {

  val schema: StructType = StructType(
    List(
      StructField("id", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("address", StringType, nullable = true),
      StructField("lon", FloatType, nullable = true),
      StructField("lat", FloatType, nullable = true),
      StructField("elevation", IntegerType, nullable = true)
    )
  )

  private def nullConverter(r: String) = {
    if (r.length() == 0) null else r
  }

  private def nullConverterList(r: List[String]) = {
    r.map(r => nullConverter(r))
  }

  private def convertToInt(r: String) = {
    try {
      r.toInt
    } catch {
      case _: Exception => 0
    }
  }

  def extractSeas(input: RDD[SparkPubsubMessage]): RDD[Row] = {
    input.map(message => new String(message.getData(), StandardCharsets.UTF_8))
      .filter(_.length != 0)
      .map(_.split(""",(?=(?:[^"]*"[^"]*")*[^"]*$)"""))
      .map {
        attribute =>
          nullConverterList(attribute.take(11).toList) :::
            List(convertToInt(attribute(11))) :::
            nullConverterList(attribute.takeRight(11).toList)
      }
      .map(attribute => Row.fromSeq(attribute))
  }
}
