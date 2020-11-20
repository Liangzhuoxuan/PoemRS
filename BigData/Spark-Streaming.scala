import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import com.alibaba.fastjson.JSON

// 注意配置 Maven
object FlumePushStreaming {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[3]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(1))
    val stream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createStream(ssc, "localhost", 1234)
    // 取出 headers 部分
    val dstream: DStream[String] = stream.map(x=>x.event.getHeaders.toString)
    //处理 json 字符串
    val dataRDD = dstream.map(json => {
      // 对原来的json字符串进行修正
      val first:String = json.replaceAll("=", "\":\"")
        .replaceAll(",", "\",\"")
        .replaceAll("[{]", "{\"")
        .replaceAll("[}]", "\"}")
        .replaceAll(" ", "")

      println(first)
      val fixedJson = first
      val jsonObject = JSON.parseObject(fixedJson)
      val userId = jsonObject.getOrDefault("userId", null)
      val age = jsonObject.getOrDefault("age", null)
      val name = jsonObject.getOrDefault("name", null)
      val itemId = jsonObject.getOrDefault("itemId", null)
      val click = jsonObject.getOrDefault("click", null)
      println((userId, age, name, itemId, click))
      (userId, age, name, itemId, click)
    })
    dataRDD.print()

    //开启计算
    ssc.start()
    ssc.awaitTermination()
  }
}
