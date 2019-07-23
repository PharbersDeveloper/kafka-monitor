package com.pharbers.kafka.monitor.action

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
case class KafkaMsgAction(topic: String, id: String) extends Action with Runnable{
    lazy val producer = new PharbersKafkaProducer[String, MonitorResponse]
    var msg: String = "0"
    var producerOpen = false

    override def start(): Unit = {
        RootLogger.logger.info("开始建立producer")
        producer.produce(topic, s"$id", new MonitorResponse(id, 0L, ""))
        producerOpen = true
        new Thread(this).start()
    }

    override def runTime(msg: String): Unit = {
        RootLogger.logger.debug(msg)
        if (this.msg.toLong < msg.toLong) this.msg = msg
    }

    override def end(): Unit = {
        producerOpen = false
        RootLogger.logger.info("KafkaMsgAction end")
    }

    override def error(errorMsg: String): Unit = {
        RootLogger.logger.info(s"发送错误信息：$errorMsg")
        Thread.sleep(3000)
        producer.produce(topic, s"$id:error", new MonitorResponse(id, 100L, errorMsg))
    }

    def run(): Unit ={
        while(producerOpen) {
            Thread.sleep(3000)
            RootLogger.logger.info(s"发送kafka msg: $msg")
            producer.produce(topic, s"$id:run", new MonitorResponse(id, msg.toLong, ""))
        }
        RootLogger.logger.info(s"退出线程")
    }
}
