package com.msb.test

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.time.Time
import org.apache.flink.cep.scala.pattern.Pattern
//import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.scala.CEP
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.parsing.json.JSONObject

/**
 * @author xyh
 * @date 2022/6/13 0:03
 **/
object CEPTest {

  def main(args: Array[String]): Unit = {

    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置连接kafka的配置信息
    val props = new Properties()
    //注意   sparkstreaming + kafka（0.10之前版本） receiver模式  zookeeper url（元数据）
    props.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    props.setProperty("group.id", "flink-kafka-001")
    props.setProperty("key.deserializer", classOf[StringSerializer].getName)
    props.setProperty("value.deserializer", classOf[StringSerializer].getName)

    val stream = environment.addSource(new FlinkKafkaConsumer[String]("flink-kafka", new SimpleStringSchema(), props))

    val start = Pattern.begin[JSONObject]("start_pattern")
//    start.where(_.getIP = "0.0.0.0").times(100).within(10)

    start.where(_.equals()).times(100).within(windowtime)
    val patternStream = CEP.pattern[JSONObject](stream,start)




    environment.execute()

  }

}
