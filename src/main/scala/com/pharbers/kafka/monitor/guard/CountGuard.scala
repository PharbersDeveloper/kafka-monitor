package com.pharbers.kafka.monitor.guard

import com.pharbers.kafka.monitor.action.Action
import com.pharbers.kafka.monitor.exception.HttpRequestException
import com.pharbers.kafka.monitor.httpClient.JsonMode.QueryMode
import com.pharbers.kafka.monitor.httpClient.{BaseHttpClient, HttpClient}
import com.pharbers.kafka.monitor.manager.BaseGuardManager
import com.pharbers.kafka.monitor.util.{JsonHandler, KsqlRunner, RootLogger}

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/11 11:05
  * @note 一些值得注意的地方
  */
case class CountGuard(id: String, url: String, action: Action) extends Guard {
    private var open = false
    private val sqlId = id.replaceAll("-", "")

    override def init(): Unit = {
        val createStream = "create stream source_stream_" + sqlId + " with (kafka_topic = '" + s"source_$id" + "', value_format = 'avro');"
        //        val createSourceTable = "\"create table " + id + "_table as select count(*) as count from " + id + "_stream group by rowkey;\"," + " \"streamsProperties\": {\n" + "\"ksql.streams.auto.offset.reset\": \"earliest\"}"
        val createSinkTable = "create stream sink_stream_" + sqlId + " with (kafka_topic = '" + s"recall_$id" + "', value_format = 'avro');"
        //todo: 配置文件读取url
        //todo: 结果判断
        try {
            val createStreamResponse = KsqlRunner.runSql(createStream, s"$url/ksql")
            println(createStreamResponse.readLine())
        } catch {
            case e: Exception => {
                open = false
                action.error(s"create stream error: ${e.getMessage}")
                throw e
            }
        }
        Thread.sleep(5000)
        try {
            val createSinkTableResponse = KsqlRunner.runSql(createSinkTable, s"$url/ksql")
            println(createSinkTableResponse.readLine())
        } catch {
            case e: HttpRequestException => {
                open = false
                KsqlRunner.runSql(s"drop stream source_stream_$sqlId;", s"$url/ksql")
                action.error(s"create stream error: ${e.getMessage}")
                throw e
            }
        }
    }

    override def run(): Unit = {
        action.start()
        open = true
        var sourceCount = 0L
        var sinkCount = 0L
        var trueCount = 10

        val selectSourceCount = s"select count(*) from source_stream_$sqlId group by jobId;"
        val selectSinkCount = s"select sum(count) from sink_stream_$sqlId group by jobId;"
        //todo: 配置文件读取url
        //todo: 用jackson生成ksql
        val sourceRead = try {
            KsqlRunner.runSql(selectSourceCount, s"$url/query")
        } catch {
            case e: HttpRequestException => {
                close()
                action.error(s"ksql query error: ${e.getMessage}")
                throw e
            }
        }
        val sinkRead = try {
            KsqlRunner.runSql(selectSinkCount, s"$url/query")
        } catch {
            case e: HttpRequestException => {
                close()
                action.error(s"ksql query error: ${e.getMessage}")
                throw e
            }
        }
        while (isOpen) {
            val sourceRes = sourceRead.readLine
            if (sourceRes.length > 0) {
                val row = JsonHandler.readObject[QueryMode](sourceRes).row
                sourceCount = row.getColumns.get(0).toLong
            }
            val sinkRes = sinkRead.readLine
            if (sinkRes.length > 0) {
                val row = JsonHandler.readObject[QueryMode](sinkRes).row
                sinkCount = row.getColumns.get(0).toLong
            }
            if (sourceCount == sourceCount) {
                action.runTime("99")
                trueCount = trueCount - 1
            } else {
                action.runTime((sourceCount / sinkCount * 100).toString)
            }
            if (trueCount == 0) {
                action.runTime("100")
                action.end()
                close()
            }
        }
    }

    override def close(): Unit = {
        val dropTables = List(s"drop stream source_stream_$sqlId;", s"drop stream sink_stream_$sqlId;")
        //todo: 结果判断
        try {
            dropTables.map(x => KsqlRunner.runSql(x, s"$url/ksql"))
        }catch {
            case e: HttpRequestException => RootLogger.logger.error(e.getMessage)
            case e: Exception =>
                RootLogger.logger.fatal(e.getMessage)
                throw e
        }
        open = false
    }

    override def isOpen: Boolean = {
        open
    }

}
