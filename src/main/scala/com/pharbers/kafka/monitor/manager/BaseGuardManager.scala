package com.pharbers.kafka.monitor.manager

import com.pharbers.kafka.monitor.guard.Guard
import com.pharbers.kafka.monitor.util.RootLogger

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
        if (guardMap.size > 100) {
            clean()
        }
        guardMap.getOrElse(id, {
            guardMap.put(id, guard)
            guard
        })
    }

    override def getGuard(id: String): Guard = {
        guardMap.getOrElse(id, new Guard {
            override def init(): Unit = {}

            override def run(): Unit = {}

            override def close(): Unit = {}

            override def isOpen: Boolean = false
        })
    }

    override def openGuard(id: String): Unit = {
        if (guardMap.contains(id) && !guardMap(id).isOpen) {
            guardMap(id).init()
            RootLogger.logger.info(s"start guard $id")
            new Thread(guardMap(id)).start()
        }
    }

    def closeAll(): Unit = {
        guardMap.values.foreach(x => if (x.isOpen) x.close())
        guardMap.clear()
    }

    def clean(): Unit = {
        guardMap.keys.foreach(x => if (!guardMap(x).isOpen) guardMap.remove(x))
    }
}
