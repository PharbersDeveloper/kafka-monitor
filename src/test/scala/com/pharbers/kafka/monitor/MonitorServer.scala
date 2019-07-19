package com.pharbers.kafka.monitor

import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.monitor.action.{Action, KafkaMsgAction}
import com.pharbers.kafka.monitor.guard.CountGuard
import com.pharbers.kafka.monitor.manager.BaseGuardManager
import com.pharbers.kafka.monitor.util.RootLogger
import com.pharbers.kafka.schema.MonitorRequest
import org.apache.kafka.clients.consumer.ConsumerRecord


/**
  * @ ProjectName kafka-monitor.com.pharbers.kafka.monitor.monotorDaemon
  * @ author jeorch
  * @ date 19-7-12
  * @ Description: TODO
  */
object MonitorServer extends App {


    //step 0 开启Kafka-Consumer接收 需要监控的Job信息（参数[JobID]和[监控策略]）
    val pkc = new PharbersKafkaConsumer[String, MonitorRequest](List(monitor_config_obj.REQUEST_TOPIC), 1000, Int.MaxValue, monitorProcess)
    val t = new Thread(pkc)
    try {
        RootLogger.logger.info("MonitorServer starting!")
        t.start()

        RootLogger.logger.info("MonitorServer is started! Close by enter \"exit\" in console.")
        var cmd = Console.readLine()
        while (cmd != "exit") {
            cmd = Console.readLine()
        }

    } catch {
        case ie: InterruptedException => {
            RootLogger.logger.error(ie.getMessage)
        }
    } finally {
        pkc.close()
        BaseGuardManager.closeAll()
        RootLogger.logger.error("MonitorServer close!")
    }

    def monitorProcess(record: ConsumerRecord[String, MonitorRequest]): Unit = {

        record.value().getStrategy.toString match {
            case "default" => {
                doDefaultMonitorFunc(record.value().getJobId.toString, monitor_config_obj.RESPONSE_TOPIC)
            }
//            case ??? => ???
        }

        RootLogger.logger.info("===myProcess>>>" + record.key() + ":" + record.value())
    }

    def doDefaultMonitorFunc(jobId: String, topic: String): Unit = {
        val action =  KafkaMsgAction(topic, jobId)
//        val action = new Action() {
//            override def start(): Unit = {
//                println("start")
//            }
//
//            override def runTime(msg: String): Unit = {
//                println(msg)
//            }
//
//            override def end(): Unit = {
//                println("end")
//            }
//
//            override def error(errorMsg: String): Unit = {
//                println(errorMsg)
//            }
//        }
        try{
            BaseGuardManager.createGuard(jobId, CountGuard(jobId, "http://59.110.31.50:8088", action))
            BaseGuardManager.openGuard(jobId)
        }catch {
            case e: Exception => RootLogger.logger.error("创建监控失败", e)
        }
    }

}
