package com.xq.learn.api.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * Flink中的Source，可以从不同的数据源读取数据，比如list，文件，socket等
 */
object SourceDemo {
  def main(args: Array[String]): Unit = {
    // 1. 获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2. 获取数据源
    // 2.1 从集合中获取数据源
    val dataList = List(
      SensorReader("sensor_1", System.currentTimeMillis(), 12.3),
      SensorReader("sensor_2", System.currentTimeMillis(), 12.6)
    )
    val listDataStream = env.fromCollection(dataList)
    listDataStream.print()

    // 2.2 从文件中读取数据
    val filePath = "src/main/resources/sensor.csv"
    val fileDataStream = env.readTextFile(filePath)
    fileDataStream.print()

    // 2.3 从kafka steam中读取数据
    val topic = "sensor"
    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")
    val kafkaDataStream = env.addSource(new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), props))
    kafkaDataStream.print()


    // 3. 流处理程序必须执行
    env.execute("source-demo")
  }
}

case class SensorReader(id: String, timestamp: Long, temperature: Double)

/**
 * 自定义source类型，必须重写run(), cancel()两个方法
 */
class MySource extends RichSourceFunction[String] {
  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {

    // collect方法生成数据
    sourceContext.collect("test")
  }

  override def cancel(): Unit = ???
}