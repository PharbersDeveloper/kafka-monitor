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
case class KafkaMsgAction(topic: String, id: String) extends Action with Runnable with Cloneable{
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
        if (this.msg.toLong < msg.toLong && msg.toLong <= 100) this.msg = msg
    }

    override def end(): Unit = {
        producerOpen = false
        RootLogger.logger.info(s"发送结束信息：jobId:$id, msg: $msg")
        producer.produce(topic, s"$id:end", new MonitorResponse(id, msg.toLong, ""))
        RootLogger.logger.info("KafkaMsgAction end")
        producer.producer.close(Duration.ofSeconds(10))
    }

    override def error(errorMsg: String): Unit = {
        RootLogger.logger.info(s"发送错误信息：jobId:$id, error:$errorMsg")
        producer.produce(topic, s"$id:error", new MonitorResponse(id, 100L, errorMsg))
    }

    override def cloneAction(): Action = {
        val res = KafkaMsgAction(topic, id)
        res.msg = msg
        res
    }

    def run(): Unit ={
        while(producerOpen) {
            RootLogger.logger.info(s"发送kafka, jobId:$id, msg: $msg")
            producer.produce(topic, s"$id:run", new MonitorResponse(id, msg.toLong, ""))
            Thread.sleep(3000)
        }
        RootLogger.logger.info(s"退出线程")
    }

}
