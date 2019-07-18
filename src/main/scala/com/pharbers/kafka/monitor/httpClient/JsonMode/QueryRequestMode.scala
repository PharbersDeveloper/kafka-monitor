package com.pharbers.kafka.monitor.httpClient.JsonMode

import scala.beans.BeanProperty

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/11 19:29
  * @note 一些值得注意的地方
  */
case class QueryRequestMode() {
    @BeanProperty
    var ksql: String = ""
    @BeanProperty
    var streamsProperties: java.util.Map[String, String] = new java.util.HashMap()
//    streamsProperties.put("ksql.streams.auto.offset.reset", "earliest")
//    streamsProperties.put("ksql.streams.auto.offset.reset", "latest")
}
