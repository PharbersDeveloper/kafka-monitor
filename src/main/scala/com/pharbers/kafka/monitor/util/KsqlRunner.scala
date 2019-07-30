package com.pharbers.kafka.monitor.util

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.nio.charset.StandardCharsets

import com.pharbers.kafka.monitor.httpClient.{BaseHttpClient, HttpClient}
import com.pharbers.kafka.monitor.httpClient.JsonMode.QueryRequestMode

import scala.collection.JavaConversions
import com.pharbers.kafka.monitor.Config._
import com.pharbers.kafka.monitor.exception.HttpRequestException
import org.apache.logging.log4j.{LogManager, Logger}

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
    private val logger: Logger = LogManager.getLogger(this.getClass)

    def runSql(sql: String, url: String, map: Map[String, String], maxReCallNum: Int = config.get("ksqlHttpClient").get("defaultMaxRecall").asInt(20)): BufferedReader = {
        val ksql = QueryRequestMode()
        ksql.setKsql(sql)
        ksql.setStreamsProperties(JavaConversions.mapAsJavaMap(map))
        val ksqlJson = JsonHandler.writeJson(ksql)
        val contentType = "application/vnd.ksql.v1+json"
        val httpClient: HttpClient = HttpClient(config.get("ksqlHttpClient").get("name").asText("BaseHttpClient"))
                .build(Map("url" -> url))
        try {
            new BufferedReader(new InputStreamReader(httpClient.post(ksqlJson, contentType), StandardCharsets.UTF_8))
        } catch {
            case e: Exception =>
                val reCallNum = maxReCallNum - 1
                if (reCallNum > 0) {
                    logger.info(s"recall, 还能: $reCallNum 次, maxRecallNum: $maxReCallNum")
                    Thread.sleep(10000)
                    runSql(sql, url, map, reCallNum)
                } else {
                    logger.error(s"recall $reCallNum 次后失败， maxRecallNum: $maxReCallNum")
                    throw e
                }
        }
    }

    def asynchronousTunSql(sql: String, url: String, map: Map[String, String], maxReCallNum: Int = config.get("ksqlHttpClient").get("defaultMaxRecall").asInt(20)): Unit ={

    }
}
