//package com.pharbers.kafka.monitor
//
//import com.pharbers.kafka.consumer.PharbersKafkaConsumer
//import com.pharbers.kafka.monitor.action.{Action, KafkaMsgAction}
//import com.pharbers.kafka.monitor.guard.CountGuard
//import com.pharbers.kafka.monitor.manager.{BaseGuardManager, GuardManager}
//import com.pharbers.kafka.schema.MonitorRequest
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.logging.log4j.LogManager
//
//
///**
//  * @ ProjectName kafka-monitor.com.pharbers.kafka.monitor.monotorDaemon
//  * @ author jeorch
//  * @ date 19-7-12
//  * @ Description: TODO
//  */
//object MonitorServer extends App {
//    private val logger = LogManager.getLogger(this.getClass)
//    val guardManager: GuardManager = BaseGuardManager
//    //step 0 开启Kafka-Consumer接收 需要监控的Job信息（参数[JobID]和[监控策略]）
//    val pkc = new PharbersKafkaConsumer[String, MonitorRequest](List(monitor_config_obj.REQUEST_TOPIC), 1000, Int.MaxValue, monitorProcess)
//    val t = new Thread(pkc)
//    try {
//        logger.info("MonitorServer starting!")
//        t.start()
//
//        logger.info("MonitorServer is started! Close by enter \"exit\" in console.")
//        var cmd = Console.readLine()
//        while (cmd != "exit") {
//            cmd = Console.readLine()
//        }
//
//    } catch {
//        case ie: InterruptedException => {
//            logger.error(ie.getMessage)
//        }
//    } finally {
//        pkc.close()
//        guardManager.clean()
//        logger.error("MonitorServer close!")
//    }
//
//    def monitorProcess(record: ConsumerRecord[String, MonitorRequest]): Unit = {
//
//        record.value().getStrategy.toString match {
//            case "default" =>
//                doDefaultMonitorFunc(record.value().getJobId.toString, monitor_config_obj.RESPONSE_TOPIC)
//            case "close" => closeOneGuard(record.value().getJobId.toString)
//            case "closeAll" => closeAllGuard()
//
//            //            case ??? => ???
//        }
//
//        logger.info("===myProcess>>>" + record.key() + ":" + record.value())
//    }
//
//    def doDefaultMonitorFunc(jobId: String, topic: String): Unit = {
//        logger.info(s"jobid; $jobId; 开始创建监控")
//        val action =  KafkaMsgAction(topic, jobId, "")
//        try{
//            guardManager.createGuard(jobId, CountGuard(jobId, "http://59.110.31.50:8088", action))
//            guardManager.openGuard(jobId)
//        }catch {
//            case e: Exception => logger.error("创建监控失败", e)
//        }
//    }
//
//    def closeOneGuard(jobId: String): Unit ={
//        logger.info(s"关闭相关监控， jodId: $jobId")
//        guardManager.close(jobId)
//    }
//
//    def closeAllGuard(): Unit ={
//        guardManager.clean()
//    }
//
//}
