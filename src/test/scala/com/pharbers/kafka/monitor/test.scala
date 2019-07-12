package com.pharbers.kafka.monitor

import com.pharbers.kafka.monitor.action.Action
import com.pharbers.kafka.monitor.guard.CountGuard
import com.pharbers.kafka.monitor.manager.BaseGuardManager
import org.scalatest.FunSuite


object test extends App {
    val jobId = "dcs_test1"
    val topic = "dcs008"
    val action =  new Action() { override def exec(): Unit = println("********************ok*******************") }
    BaseGuardManager.createGuard(jobId, CountGuard(jobId, topic, "http://59.110.31.50:8088", action))
    BaseGuardManager.openGuard(jobId)
}