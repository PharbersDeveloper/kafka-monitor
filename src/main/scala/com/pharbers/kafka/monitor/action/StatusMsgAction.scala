package com.pharbers.kafka.monitor.action

import java.time.Duration

import com.pharbers.kafka.monitor.util.RootLogger
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema. MonitorResponse2

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/08/27 15:03
  * @note 一些值得注意的地方
  */
case class StatusMsgAction(topic: String) extends Action{
    var producer: PharbersKafkaProducer[String, MonitorResponse2] = _
    var msg: String = "0"

    override def start(): Unit = {
        RootLogger.logger.info("开始建立producer")
        producer = new PharbersKafkaProducer[String, MonitorResponse2]
    }

    override def runTime(msg: String): Unit = {
    }

    override def end(): Unit = {
        producer.producer.flush()
        producer.producer.close(Duration.ofSeconds(10))
    }

    override def error(errorMsg: String): Unit = {
        val connectorName = errorMsg.split("#").head
        RootLogger.logger.info(s"发送错误信息：connectorName:$connectorName, error:failed")
        producer.produce(topic, s"$connectorName", new MonitorResponse2(connectorName, "", -1L, errorMsg))
    }

    override def cloneAction(): Action = {
        val res = StatusMsgAction(topic)
        res.msg = msg
        res
    }

}
