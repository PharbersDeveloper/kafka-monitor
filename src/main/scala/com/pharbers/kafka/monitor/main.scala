package com.pharbers.kafka.monitor

import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.monitor.action.{Action, KafkaMsgAction}
import com.pharbers.kafka.monitor.guard.{CountGuard, TmGuard}
import com.pharbers.kafka.monitor.manager.{BaseGuardManager, GuardManager}
import com.pharbers.kafka.schema.MonitorRequest
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.logging.log4j.{LogManager, Logger}
import com.pharbers.kafka.monitor.Config._

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
    val guardManager: GuardManager = BaseGuardManager
    def main(args: Array[String]): Unit = {
        val pkc = new PharbersKafkaConsumer[String, MonitorRequest](List(config.get("consumerTopic").asText()), 1000, Int.MaxValue, monitorProcess)
        val t = new Thread(pkc)
        try {
            logger.info("MonitorServer starting!")
            t.start()

            logger.info("MonitorServer is started! Close by enter \"exit\" in console.")
            var cmd = Console.readLine()
            while (cmd != "exit") {
                cmd = Console.readLine()
            }

        } catch {
            case ie: InterruptedException => {
                logger.error(ie.getMessage)
            }
        } finally {
            pkc.close()
            guardManager.clean()
            logger.error("MonitorServer close!")
        }
    }
    def monitorProcess(record: ConsumerRecord[String, MonitorRequest]): Unit = {

        record.value().getStrategy.toString match {
            case "default" =>
                doDefaultMonitorFunc(record.value().getJobId.toString, config.get("producerTopic").asText())
            case "close" => closeOneGuard(record.value().getJobId.toString)
            case "closeAll" => closeAllGuard()

            //            case ??? => ???
        }

        logger.info("===myProcess>>>" + record.key() + ":" + record.value())
    }

    def doDefaultMonitorFunc(jobId: String, topic: String): Unit = {
        logger.info(s"jobid; $jobId; 开始创建监控")
        val action =  KafkaMsgAction(topic, jobId)
        try{
//            guardManager.createGuard(jobId, CountGuard(jobId, "http://59.110.31.50:8088", action))
            guardManager.createGuard(jobId, TmGuard(jobId, "http://59.110.31.50:8088", action))
            guardManager.openGuard(jobId)
        }catch {
            case e: Exception => logger.error("创建监控失败", e)
        }
    }

    def closeOneGuard(jobId: String): Unit ={
        logger.info(s"关闭相关监控， jodId: $jobId")
        guardManager.close(jobId)
    }

    def closeAllGuard(): Unit ={
        guardManager.clean()
    }
}
