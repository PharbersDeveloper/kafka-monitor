package com.pharbers.kafka.monitor

import java.io.BufferedReader
import java.net.SocketTimeoutException

import com.pharbers.kafka.monitor.action.KafkaMsgAction
import com.pharbers.kafka.monitor.guard.CountGuard
import com.pharbers.kafka.monitor.httpClient.JsonMode.QueryMode
import com.pharbers.kafka.monitor.manager.BaseGuardManager
import com.pharbers.kafka.monitor.util.{JsonHandler, KsqlRunner, RootLogger}


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

object testKsqlCount extends App {
    val id = "c304e36134394921b321339a8ac7331a"
    val count = 1
    (1 to count).map { x =>
        RootLogger.logger.debug(s"第${x}次start")
        val createStream = s"create stream stream$x$id with " + s"(kafka_topic = 'source_$id', value_format = 'avro');"
        val query = s"select count(*) as count from stream$x$id  group by jobid;"
        KsqlRunner.runSql(createStream, "http://59.110.31.50:8088/ksql", Map("ksql.streams.auto.offset.reset" -> "earliest"))
        val reader = KsqlRunner.runSql(query, "http://59.110.31.50:8088/query", Map("ksql.streams.auto.offset.reset" -> "earliest"))
        reader
    }.zipWithIndex.foreach{reader =>
        var open = true
        while (open){
            var ref = 0L
            val json = if (reader._1.ready()) {
                reader._1.readLine
            } else {
                ""
            }
            if (json.length > 0) {
                val row = JsonHandler.readObject[QueryMode](json).row
                try {
                    ref = row.getColumns.get(0).toLong
                } catch {
                    case e: Exception =>
                        RootLogger.logger.debug(e)
                }
            }
            if (ref == 178485L) {
                RootLogger.logger.debug("成功", s"第${reader._2}次")
                open = false
            } else if (ref > 178485L) {
                RootLogger.logger.error(s"失败, count: ${ref}第${reader._2}次")
                open = false
            }
        }
    }


}