package com.pharbers.kafka.monitor.manager

import com.pharbers.kafka.monitor.guard.Guard

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/11 10:52
  * @note 一些值得注意的地方
  */
trait GuardManager {
    def createGuard(id: String, monitor: Guard): Guard

    def getGuard(id: String): Guard

    def openGuard(id: String)

    def closeAll()

    def clean()
}
