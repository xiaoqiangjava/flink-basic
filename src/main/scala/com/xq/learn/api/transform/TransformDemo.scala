package com.xq.learn.api.transform

import com.xq.learn.api.source.SensorReader
import org.apache.flink.streaming.api.scala._

/**
 * Flink中的转换算子
 */
object TransformDemo {
  def main(args: Array[String]): Unit = {
    // 0. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 1. 读取数据
    val inputPath = "src/main/resources/sensor.csv"
    val records: DataStream[String] = env.readTextFile(inputPath)

    // 2. 使用基本算子转换成case class类型
    val sensorDS: DataStream[SensorReader] = records.map(row => {
      val arr = row.split(",")
      SensorReader(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    // 3. 分组聚合，输出当前每个传感器当前温度最小值，分组之后一般会跟滚动聚合算子
    val aggStream = sensorDS.keyBy(_.id)
      .min("temperature")

    aggStream.print()

    // 4. 需要输出当前最小的温度值以及最近的时间戳，使用reduce，reduce之前必须调用keyBy, reduce规约的第一个参数是上次规约之后的结果作为参数，
    // 第二个参数是新的数据
    val reduceStream = sensorDS.keyBy(_.id)
        .reduce((last, cur) => SensorReader(last.id, cur.timestamp, last.temperature.min(cur.temperature)))

    reduceStream.print()

    // 5. 分流操作split，将温度按照20为界限分流, DataStream -> SplitStream, 使用select操作将SplitStream -> DataStream
    val splitStream = sensorDS.split(row => {
      if (row.temperature > 13) {
        Seq("high") // 相当于给定一个label
      } else {
        Seq("low")
      }
    })
    val highDS: DataStream[SensorReader] = splitStream.select("high")
    val lowDS: DataStream[SensorReader] = splitStream.select("low")
    val allDS: DataStream[SensorReader] = splitStream.select("high", "low")
    highDS.print("high")
    lowDS.print("low")
    allDS.print("all")

    // 6. 合流操作 connect, 将两条流合并到一个流，ConnectedStream, 并不是真正的合并，只是在一个流中包装了两条流，然后通过ConnectedStream
    // 的map, flatMap操作之后就会合并成一条真正的流DataStream, 这种操作交colMap或者colFlatmap, 分别处理不同的流，不同的流类型可以不同
    val warningsDS: DataStream[(String, Double)] = sensorDS.map(row => (row.id, row.temperature))
    val connectedStream: ConnectedStreams[(String, Double), SensorReader] = warningsDS.connect(highDS)
    val oneDS: DataStream[(String, Double, String)] = connectedStream
      .map(warnings => (warnings._1, warnings._2, "warning"), high => (high.id, high.temperature, "normal"))

    oneDS.print("合并之后的流")

    // 6.1 union合并两条流，必须要求流的数据类型是一样的
    val unionDS: DataStream[SensorReader] = highDS.union(lowDS)
    unionDS.print("union data stream")
    // 执行流处理
    env.execute("transform-demo")

  }
}
