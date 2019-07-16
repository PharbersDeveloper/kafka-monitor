package com.pharbers.kafka.monitor.guard

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/11 10:43
  * @note 一些值得注意的地方
  */
trait Guard extends Runnable{
    def init(): Unit

    def run(): Unit

    def close(): Unit

    def isOpen: Boolean
}
