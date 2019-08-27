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
  * @since 2019/08/27 15:03
  * @note 一些值得注意的地方
  */
case class StatusMsgAction(topic: String) extends Action{
    var producer: PharbersKafkaProducer[String, MonitorResponse] = _
    var msg: String = "0"

    override def start(): Unit = {
        RootLogger.logger.info("开始建立producer")
        producer = new PharbersKafkaProducer[String, MonitorResponse]
    }

    override def runTime(msg: String): Unit = {
    }

    override def end(): Unit = {
        producer.producer.flush()
        producer.producer.close(Duration.ofSeconds(10))
    }

    override def error(errorMsg: String): Unit = {
        val jobId = errorMsg.split("#").head
        RootLogger.logger.info(s"发送错误信息：jobId:$jobId, error:failed")
        producer.produce(topic, s"$jobId:error", new MonitorResponse(jobId, 100L, errorMsg))
    }

    override def cloneAction(): Action = {
        val res = StatusMsgAction(topic)
        res.msg = msg
        res
    }

}
