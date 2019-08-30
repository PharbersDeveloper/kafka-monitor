package com.pharbers.kafka.monitor


import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.monitor.action.{KafkaMsgAction, StatusMsgAction}
import com.pharbers.kafka.monitor.guard.{ConnectorGuard, StatusGuard}
import com.pharbers.kafka.monitor.manager.GuardManager
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.logging.log4j.{LogManager, Logger}
import com.pharbers.kafka.monitor.Config._
import com.pharbers.kafka.schema.MonitorRequest2

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/11 10:40
  * @note 一些值得注意的地方
  */
object main {
    val logger: Logger = LogManager.getLogger(this.getClass)
    def main(args: Array[String]): Unit = {
        GuardManager.createGuard(config.get("statusTopic").asText(), StatusGuard(config.get("statusTopic").asText(), StatusMsgAction(config.get("producerTopic").asText())))
        val pkc = new PharbersKafkaConsumer[String, MonitorRequest2](List(config.get("consumerTopic").asText()), 1000, Int.MaxValue, monitorProcess)
        try {
            logger.info("MonitorServer starting!")
            logger.debug("MonitorServer is started! 输入 \"exit\" 并不会发生什么.")
            pkc.run()
        } catch {
            case ie: InterruptedException => {
                logger.error(ie.getMessage)
            }
        } finally {
            pkc.close()
            GuardManager.clean()
            logger.error("MonitorServer close!")
        }
    }
    def monitorProcess(record: ConsumerRecord[String, MonitorRequest2]): Unit = {

        record.value().getStrategy.toString match {
            case "default" =>
                doDefaultMonitorFunc(record.value().getJobId.toString, record.value().getConnectorName.toString, record.value().getSourceTopic.toString, record.value().getRecallTopic.toString, config.get("producerTopic").asText())
            case "close" => closeOneGuard(record.value().getConnectorName.toString)
            case "closeAll" => closeAllGuard()
        }

        logger.info("===myProcess>>>" + record.key() + ":" + record.value())
    }

    def doDefaultMonitorFunc(jobId: String, connectorName: String, sourceTopic: String, recallTopic: String, topic: String): Unit = {
        logger.info(s"connectorName; $connectorName; 开始创建监控")
        val action =  KafkaMsgAction(topic, jobId, connectorName)
        try{
            GuardManager.createGuard(connectorName, ConnectorGuard(jobId, sourceTopic, recallTopic, action))
            GuardManager.openGuard(connectorName)
            //每次都尝试启动status监控，如果以及启动就无事发生
            GuardManager.openGuard(config.get("statusTopic").asText())
        }catch {
            case e: Exception => logger.error("创建监控失败", e)
        }
    }

    def closeOneGuard(jobId: String): Unit ={
        logger.info(s"关闭相关监控， jodId: $jobId")
        GuardManager.close(jobId)
    }

    def closeAllGuard(): Unit ={
        GuardManager.clean()
    }
}
