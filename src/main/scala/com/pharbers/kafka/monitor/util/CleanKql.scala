package com.pharbers.kafka.monitor.util

import com.fasterxml.jackson.databind.ObjectMapper

import scala.beans.BeanProperty
import scala.reflect.ClassTag

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/18 10:41
  * @note 一些值得注意的地方
  */
object CleanKql extends App {

    /** 功能描述:
      *
      * @param  name   : streams/tables/queries
      * @param  url    : http://59.110.31.50:8088
      * @param  filter : 过滤sql语句
      * @return
      * @example
      * @note
      * @history
      */
    def deleteDefinitions(name: String, url: String): Unit = {
        val show = s"show $name;"
        val jsonTree = JsonHandler.objectMapper.readTree(
            KsqlRunner.runSql(show, s"http://59.110.31.50:8088/ksql", Map("ksql.streams.auto.offset.reset" -> "earliest")).readLine())
        val elements = jsonTree.elements().next().path(s"$name").elements()
        while (elements.hasNext) {
            val deleteSql = name match {
                case "queries" => s"terminate ${elements.next().get("id").asText()};"
                case "streams" => s"drop stream ${elements.next().get("name").asText()};"
                case "tables" => s"drop table ${elements.next().get("name").asText()};"
                case _ => ""
            }
            KsqlRunner.runSql(deleteSql, s"$url/ksql", Map("ksql.streams.auto.offset.reset" -> "earliest"))
        }
    }

    def cleanAll(url: String): Unit = {
        deleteDefinitions("queries", url)
        deleteDefinitions("tables", url)
        deleteDefinitions("streams", url)
    }

    cleanAll("http://59.110.31.50:8088")
}
