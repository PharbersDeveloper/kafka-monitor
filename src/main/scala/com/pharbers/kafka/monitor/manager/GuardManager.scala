package com.pharbers.kafka.monitor.manager

import com.pharbers.kafka.monitor.Config
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

    def close(id: String)

    def clean()

    def stopGuard(id: String): Unit ={}

    def startGuard(id: String): Unit ={}
}

object GuardManager{
    val clazz = Class.forName(Config.config.get("guardManager").get("class").asText())
    def createGuard(id: String, monitor: Guard): Guard = {
        clazz.getDeclaredMethod("createGuard", classOf[String], classOf[Guard])
                .invoke(null, id, monitor)
                .asInstanceOf[Guard]
    }

    def getGuard(id: String): Guard = {
        clazz.getDeclaredMethod("getGuard", classOf[String])
                .invoke(null, id)
                .asInstanceOf[Guard]
    }

    def openGuard(id: String): Unit = {
        clazz.getDeclaredMethod("openGuard", classOf[String])
                .invoke(null, id)
    }

    def close(id: String): Unit ={
        clazz.getDeclaredMethod("close", classOf[String])
                .invoke(null, id)
    }

    def clean(): Unit ={
        clazz.getDeclaredMethod("clean")
                .invoke(null)
    }

    def stopGuard(id: String): Unit ={
        clazz.getDeclaredMethod("stopGuard", classOf[String])
                .invoke(null, id)
    }

    def startGuard(id: String): Unit ={
        clazz.getDeclaredMethod("startGuard", classOf[String])
                .invoke(null, id)
    }
}