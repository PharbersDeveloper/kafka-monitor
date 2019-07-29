package com.pharbers.kafka.monitor.action

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/11 10:50
  * @note 一些值得注意的地方
  */
trait Action {
    def start(): Unit
    def runTime(msg: String): Unit
    def end(): Unit
    def error(errorMsg: String): Unit
    def cloneAction(): Action
}
