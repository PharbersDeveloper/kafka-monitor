package com.pharbers.kafka.monitor

import com.pharbers.kafka.monitor.action.{Action, KafkaMsgAction}
import com.pharbers.kafka.monitor.guard.CountGuard
import com.pharbers.kafka.monitor.manager.BaseGuardManager


object test extends App {
    val jobId = "JobID002"
    val topic = "JobID002"
    val action = KafkaMsgAction("MonitorResponse", jobId)
//    val action = new Action {
//        override def start(): Unit = {
//            println("start")
//        }
//
//        override def runTime(msg: String): Unit = {
//            println(msg)
//        }
//
//        override def end(): Unit = {
//            println("end")
//        }
//
//        override def error(errorMsg: String): Unit = {
//            println(errorMsg)
//        }
//    }
    BaseGuardManager.createGuard(jobId, CountGuard(jobId, topic, "http://59.110.31.50:8088", action))
    BaseGuardManager.openGuard(jobId)
}