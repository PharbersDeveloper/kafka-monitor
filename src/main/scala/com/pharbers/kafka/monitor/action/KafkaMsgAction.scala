package com.pharbers.kafka.monitor.action

import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.MonitorResponse

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/15 10:29
  * @note 一些值得注意的地方
  */
case class KafkaMsgAction(topic: String, id: String) extends Action {
    lazy val producer = new PharbersKafkaProducer[String, MonitorResponse]

    override def start(): Unit = {
        producer.produce(topic, s"$id", new MonitorResponse(id, 0L, ""))
    }

    override def runTime(msg: String): Unit = {
        producer.produce(topic, s"$id:run", new MonitorResponse(id, msg.toLong, ""))
    }

    override def end(): Unit = {
        //todo: log4j2
        println("end")
    }

    override def error(errorMsg: String): Unit = {
        producer.produce(topic, s"$id:error", new MonitorResponse(id, 0L, errorMsg))
    }
}