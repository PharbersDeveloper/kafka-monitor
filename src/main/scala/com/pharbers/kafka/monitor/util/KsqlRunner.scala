package com.pharbers.kafka.monitor.util

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import com.pharbers.kafka.monitor.httpClient.{BaseHttpClient, HttpClient}
import com.pharbers.kafka.monitor.httpClient.JsonMode.QueryRequestMode
import scala.collection.JavaConversions

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/11 19:27
  * @note 一些值得注意的地方
  */
object KsqlRunner {
    def runSql(sql: String, url: String, map: Map[String, String]): BufferedReader = {
        val ksql = QueryRequestMode()
        ksql.setKsql(sql)
        ksql.setStreamsProperties(JavaConversions.mapAsJavaMap(map))
        val ksqlJson = JsonHandler.writeJson(ksql)
        val contentType = "application/vnd.ksql.v1+json"
        //todo： 根据配置选择
        val httpClient: HttpClient = new BaseHttpClient(url)
        new BufferedReader(new InputStreamReader(httpClient.post(ksqlJson, contentType), StandardCharsets.UTF_8))


    }
}
