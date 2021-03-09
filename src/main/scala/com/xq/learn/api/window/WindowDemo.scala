package com.xq.learn.api.window

import com.xq.learn.api.source.SensorReader
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Flink中的窗口：
 * 窗口是将无限流切割为有限流的一种方式，他会将流数据分发到有限大小的桶（bucket)中进行分析，[)区间
 * window类型：
 * 1. 时间窗口
 *    滚动时间窗口(Tumbling Windows)
 *      将数据依据固定的窗口长度对数据进行切分
 *      时间对齐，窗口长度固定，没有重叠
 *    滑动时间窗口(Sliding Windows)
 *      滑动窗口由固定的时间长度和滑动间隔组成
 *      窗口长度固定，可以有重叠
 *    回话窗口(Session Windows)
 *      指定时间长度timeout的间隙，一段时间没有接收到新数据就生成新的窗口
 * 2. 计数窗口
 *    滚动计数窗口
 *    滑动计数窗口
 */
object WindowDemo {
  def main(args: Array[String]): Unit = {
    // 0. 获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 1. 读取数据，使用socket读取数据, nc -lk 7777在本地开一个端口
    val dataStream = env.socketTextStream("localhost", 7777).map(row => {
      val arr = row.split(",")
      SensorReader(arr(0), arr(1).toLong, arr(2).toDouble)
    })
    // 2. 先使用keyBy操作分组，然后使用timeWindow或者countWindow开窗
    val resultStream = dataStream.map(data => (data.id, data.temperature))
      .keyBy(_._1)
//      .countWindow(100) // 计数窗口
      .timeWindow(Time.seconds(15)) // 只有一个参数表示滚动窗口，两个参数表示滑动窗口
//      .window(TumblingEventTimeWindows.of(Time.seconds(15))) // 滚动窗口
//      .window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(3))) // 滑动窗口
//      .window(EventTimeSessionWindows.withGap(Time.seconds(15))) // 会话窗口
      // 这里的返回值是一个windowStream
      .reduce((lastData, curData) => (lastData._1, lastData._2.min(curData._2)))

    resultStream.print()

    // 执行
    env.execute("window-demo")
  }
}
