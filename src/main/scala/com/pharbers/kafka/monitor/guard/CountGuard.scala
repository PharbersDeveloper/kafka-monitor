package com.pharbers.kafka.monitor.guard

import com.pharbers.kafka.monitor.action.Action
import com.pharbers.kafka.monitor.httpClient.JsonMode.QueryMode
import com.pharbers.kafka.monitor.httpClient.{BaseHttpClient, HttpClient}
import com.pharbers.kafka.monitor.util.{JsonHandler, KsqlRunner}

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/11 11:05
  * @note 一些值得注意的地方
  */
case class CountGuard(id: String, topic: String, url: String, action: Action) extends Guard {
    private var open = false

    override def init(): Unit = {
        val createStream = "create stream " + id + "_source_stream with (kafka_topic = '" + topic + "', value_format = 'avro');"
        //        val createSourceTable = "\"create table " + id + "_table as select count(*) as count from " + id + "_stream group by rowkey;\"," + " \"streamsProperties\": {\n" + "\"ksql.streams.auto.offset.reset\": \"earliest\"}"
        val createSinkTable = "create stream " + id + "_sink_stream with (kafka_topic = '" + s"$topic-Recall" + "', value_format = 'avro');"
        //todo: 配置文件读取url
        //todo: 结果判断
        val createStreamResponse = KsqlRunner.runSql(createStream, s"$url/ksql")
        val createSinkTableResponse = KsqlRunner.runSql(createSinkTable, s"$url/ksql")
    }

    override def run(): Unit = {
        action.start()
        open = true
        var sourceCount = 0L
        var sinkCount = 0L
        var trueCount = 10

        val selectSourceCount = s"select count(*) from ${id}_source_stream group by jobId;"
        val selectSinkCount = s"select sum(count) from ${id}_sink_stream group by jobId;"
        //todo: 配置文件读取url
        //todo: 用jackson生成ksql
        val sourceRead = KsqlRunner.runSql(selectSourceCount, s"$url/query")
        val sinkRead = KsqlRunner.runSql(selectSinkCount, s"$url/query")
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
        val dropTables = List(s"drop stream ${id}_source_stream;", s"drop stream ${id}_sink_stream;")
        //todo: 结果判断
        dropTables.map(x => KsqlRunner.runSql(x, s"$url/ksql"))
        open = false
    }

    override def isOpen: Boolean = {
        open
    }

}
