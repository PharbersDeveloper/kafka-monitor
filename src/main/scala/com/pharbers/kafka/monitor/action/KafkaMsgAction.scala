package com.pharbers.kafka.monitor.action

import java.time.Duration

import com.pharbers.kafka.monitor.util.RootLogger
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
case class KafkaMsgAction(topic: String, connectorName: String) extends Action with Cloneable{
    var producer: PharbersKafkaProducer[String, MonitorResponse] = _
    var msg: String = "0"
    var producerOpen = false

    override def start(): Unit = {
        RootLogger.logger.info("开始建立producer")
        producer = new PharbersKafkaProducer[String, MonitorResponse]
        //暂时不发送进度，只发结果
//        producer.produce(topic, s"$id", new MonitorResponse(id, 0L, ""))
        producerOpen = true
    }

    override def runTime(msg: String): Unit = {
        RootLogger.logger.debug(msg)
        if (this.msg.toLong < msg.toLong && msg.toLong <= 100) this.msg = msg
        producer.produce(topic, s"$connectorName", new MonitorResponse(connectorName, this.msg.toLong, ""))
    }

    override def end(): Unit = {
        producerOpen = false
        RootLogger.logger.info(s"发送结束信息：jobId:$connectorName, msg: $msg")
        producer.produce(topic, s"$connectorName", new MonitorResponse(connectorName, msg.toLong, ""))
        RootLogger.logger.info("KafkaMsgAction end")
        producer.producer.flush()
        producer.producer.close(Duration.ofSeconds(10))
    }

    override def error(errorMsg: String): Unit = {
        RootLogger.logger.info(s"发送错误信息：jobId:$connectorName, error:$errorMsg")
        producer.produce(topic, s"$connectorName", new MonitorResponse(connectorName, 100L, errorMsg))
    }

    override def cloneAction(): Action = {
        val res = KafkaMsgAction(topic, connectorName)
        res.msg = msg
        res
    }
}
