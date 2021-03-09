package com.xq.learn

import org.apache.flink.streaming.api.scala._

/**
 *
 */
object WordCountStream {
  def main(args: Array[String]): Unit = {
    // 创建流处理执行环境，这里会根据不同的执行环境创建本地或者远程集群环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(4)
    // 通过监听一个socket获取流，获取的结果封装到DataStream
    val inputDS: DataStream[String] = env.socketTextStream("localhost", 7777)
    // 进行转换操作，DataStream中没有groupBy操作，响应的API为keyBy
    val resultDS = inputDS.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)

    resultDS.print()
    // 启动任务执行
    env.execute("Stream word count")
  }
}
