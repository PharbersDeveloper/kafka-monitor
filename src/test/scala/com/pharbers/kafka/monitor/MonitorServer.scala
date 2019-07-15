package com.pharbers.kafka.monitor

import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.monitor.action.Action
import com.pharbers.kafka.monitor.guard.CountGuard
import com.pharbers.kafka.monitor.manager.BaseGuardManager
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
        println("MonitorServer starting!")
        t.start()

        println("MonitorServer is started! Close by enter \"exit\" in console.")
        var cmd = Console.readLine()
        while (cmd != "exit") {
            cmd = Console.readLine()
        }

    } catch {
        case ie: InterruptedException => {
            println(ie.getMessage)
        }
    } finally {
        t.stop()
        println("MonitorServer close!")
    }

    def monitorProcess(record: ConsumerRecord[String, MonitorRequest]): Unit = {

        record.value().strategy.toString match {
            case "default" => {
                doDefaultMonitorFunc(record.value().jobId.toString, record.value().jobId + "-source")
            }
            case ??? => ???
        }

        println("===myProcess>>>" + record.key() + ":" + record.value())
    }

    def doDefaultMonitorFunc(jobId: String, topic: String): Unit = {
        //action should be send progressMsg.
        val action =  new Action() { override def exec(): Unit = println("********************ok*******************") }
        BaseGuardManager.createGuard(jobId, CountGuard(jobId, topic, "http://59.110.31.50:8088", action))
        BaseGuardManager.openGuard(jobId)
    }

}
