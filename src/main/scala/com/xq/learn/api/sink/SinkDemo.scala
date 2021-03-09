package com.xq.learn.api.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

/**
 * Flink中sink负责将处理后的数据进行输出
 */
object SinkDemo {
  def main(args: Array[String]): Unit = {
    // 0. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 1. 读取数据
    val topic = ""
    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")
    val inputDS: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), props))
    // 2. 输出数据到kafka，这里还可以输出到es, redis等，也可以自定义sink
    inputDS.addSink(new FlinkKafkaProducer[String](topic, new SimpleStringSchema(), props))

    // 执行
    env.execute("sink-demo")
  }
}

class JdbcSink extends RichSinkFunction[String] {
  /**
   * 处理生命周期，创建连接操作
   * @param parameters parameters
   */
  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()

  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    // 这里写数据，value就是每一条数据
  }
}
