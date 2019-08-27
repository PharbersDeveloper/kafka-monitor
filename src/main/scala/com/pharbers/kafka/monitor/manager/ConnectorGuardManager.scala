package com.pharbers.kafka.monitor.manager
import com.pharbers.kafka.monitor.guard.Guard

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/08/27 13:34
  * @note 一些值得注意的地方
  */
object ConnectorGuardManager extends GuardManager {
    override def createGuard(id: String, monitor: Guard): Guard = ???

    override def getGuard(id: String): Guard = ???

    override def openGuard(id: String): Unit = ???

    override def close(id: String): Unit = ???

    override def clean(): Unit = ???
}
