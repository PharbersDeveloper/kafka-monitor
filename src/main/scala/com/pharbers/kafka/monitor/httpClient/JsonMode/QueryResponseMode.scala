package com.pharbers.kafka.monitor.httpClient.JsonMode

import scala.beans.BeanProperty

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/11 15:22
  * @note 一些值得注意的地方
  */
class QueryMode {
    @BeanProperty
    var row: row = _
    @BeanProperty
    var errorMessage: String = ""
    @BeanProperty
    var finalMessage: String = ""
    @BeanProperty
    var terminal: Boolean = false
}

class row {
    @BeanProperty
    var columns: java.util.List[String] = _
}