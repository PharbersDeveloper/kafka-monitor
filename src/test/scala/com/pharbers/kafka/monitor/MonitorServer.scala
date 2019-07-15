package com.pharbers.kafka.monitor

import java.lang.InterruptedException

import com.pharbers.kafka.common.kafka_config_obj
import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
  * @ ProjectName kafka-monitor.com.pharbers.kafka.monitor.monotorDaemon
  * @ author jeorch
  * @ date 19-7-12
  * @ Description: TODO
  */
object MonitorServer extends App {

    //step 0 开启Kafka-Consumer接收 需要监控的Job信息（参数[JobID]和[监控策略]）

    val pkc = new PharbersKafkaConsumer[String, Array[Byte]](List("test"), 1000, Int.MaxValue, monitorProcess)
//    val pkc = new PharbersKafkaConsumer[String, Array[Byte]](List(monitor_config_obj.REQUEST_TOPIC), 1000, Int.MaxValue, myProcess)
    val t = new Thread(pkc)
    try {
        println("MonitorServer start!")
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




    def monitorProcess[K, V](record: ConsumerRecord[K, V]): Unit = {

//        record match {
//            case
//        }

        println("===myProcess>>>" + record.key() + ":" + new String(record.value().asInstanceOf[Array[Byte]]))
    }

}
