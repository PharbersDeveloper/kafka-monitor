package com.pharbers.kafka.monitor

import com.pharbers.kafka.monitor.action.{Action, KafkaMsgAction}
import com.pharbers.kafka.monitor.guard.CountGuard
import com.pharbers.kafka.monitor.manager.BaseGuardManager
import com.pharbers.kafka.monitor.util.KsqlRunner


object test extends App {
    val jobId = "a00584d59c3b4fdd8930b07d9ce4a9c3"
    val action = KafkaMsgAction("MonitorResponse", jobId)
    BaseGuardManager.createGuard(jobId, CountGuard(jobId, "http://59.110.31.50:8088", action))
    BaseGuardManager.openGuard(jobId)
}


object testSql extends App {
    val sql = "select * from test limit 10;"
    val reader = KsqlRunner.runSql(sql, s"http://59.110.31.50:8088/query", Map("ksql.streams.auto.offset.reset" -> "earliest"))
    while (true) {
        val a = reader.readLine()
        if (a == null) {
            println("ok")
        }
        println(reader.readLine())
    }
}