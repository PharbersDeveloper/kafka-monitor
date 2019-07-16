package com.pharbers.kafka.monitor.util

import org.apache.logging.log4j.{LogManager, Logger}

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/16 14:05
  * @note 一些值得注意的地方
  */
object RootLogger {
    val logger: Logger = LogManager.getRootLogger
}
