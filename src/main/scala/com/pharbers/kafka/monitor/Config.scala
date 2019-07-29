package com.pharbers.kafka.monitor

import java.io.File

import com.fasterxml.jackson.databind.JsonNode
import com.pharbers.kafka.monitor.util.JsonHandler

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/29 14:23
  * @note 一些值得注意的地方
  */
object Config {
    val config: JsonNode = JsonHandler.objectMapper.readTree(new File("pharbers_config/config.json"))
}
