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
        val createStream = "create table " + id + "_stream with (kafka_topic = '" + topic + "', value_format = 'avro'), key = 'job_id;"
        //        val createSourceTable = "\"create table " + id + "_table as select count(*) as count from " + id + "_stream group by rowkey;\"," + " \"streamsProperties\": {\n" + "\"ksql.streams.auto.offset.reset\": \"earliest\"}"
        val createSinkTable = "create table " + id + "_sink_table with (kafka_topic = '" + s"${topic}_recall" + "', value_format = 'avro', key = 'job_id';"
        //todo: 配置文件读取url
        //todo: 结果判断
        val createStreamResponse = KsqlRunner.runSql(createStream, s"$url/ksql")
        val createSinkTableResponse = KsqlRunner.runSql(createSinkTable, s"$url/ksql")
    }

    override def run(): Unit = {
        open = true
        var sourceCount = 0L
        var sinkCount = 0L
        var trueCount = 10

        val selectSourceCount = "select count(*) from " + s"${id}_stream group by rowkey;"
        val selectSinkCount = "select sum(count) from" + s"${id}_sink_table " + "group by job_id;"
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
            if(sourceCount == sourceCount) trueCount = trueCount - 1
            if(trueCount == 0) {
                action.exec()
                close()
            }
        }
    }

    override def close(): Unit = {
        //todo: 删除创建的表和stream以及query
        val httpClient: HttpClient = new BaseHttpClient(s"$url/ksql")
        val closeKsql = s"show queries "
        open = false
    }

    override def isOpen: Boolean = {
        open
    }

}
