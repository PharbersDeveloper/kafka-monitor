package com.pharbers.kafka.monitor.manager

import com.pharbers.kafka.monitor.guard.Guard

import scala.collection.mutable

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/11 11:28
  * @note 一些值得注意的地方
  */
object BaseGuardManager extends GuardManager {
    private val guardMap = mutable.Map[String, Guard]()

    override def createGuard(id: String, guard: Guard): Guard = {
        guardMap.getOrElse(id, {
            guardMap.put(id, guard)
            guard
        })
    }

    override def getGuard(id: String): Guard = {
        guardMap.getOrElse(id, throw new Exception("no this id"))
    }

    override def openGuard(id: String): Unit = {
        if (guardMap.contains(id) && !guardMap(id).isOpen) {
            //todo: akka
            guardMap(id).init()
            guardMap(id).run()
        }
    }
}
