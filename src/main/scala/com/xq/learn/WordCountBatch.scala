package com.xq.learn

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * 使用Flink实现批处理word count
 */
object WordCountBatch {
  def main(args: Array[String]): Unit = {
    // 创建一个批处理的执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // 从文件中读取数据
    val filePath = "src/main/resources/wc.txt"
    val inputDS: DataSet[String] = env.readTextFile(filePath)
    // 对数据进行转换处理统计
    val wc: AggregateDataSet[(String, Int)] = inputDS.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
    wc.print()

  }
}
