package com.pharbers.kafka.monitor.manager
import java.util.concurrent.Executors
import com.pharbers.kafka.monitor.Config._
import com.pharbers.kafka.monitor.guard.Guard
import com.pharbers.kafka.monitor.util.RootLogger

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
    //多并发时线程不安全，比如同时加入相同id
    private var guardMap = Map[String, Guard]()
    private val executorService = Executors.newFixedThreadPool(config.get("guardManager").get("maxThread").asInt())
    private val emptyGuard =  new Guard {
        override def init(): Unit = {}

        override def run(): Unit = {}

        override def close(): Unit = {}

        override def isOpen: Boolean = false
    }

    override def createGuard(id: String, guard: Guard): Guard = {
        if (guardMap.size > 100) {
            guardMap = guardMap.filter(x => x._2.isOpen)
        }
        guardMap.getOrElse(id, {
            guardMap = guardMap ++ Map(id -> guard)
            guard
        })
    }

    override def getGuard(id: String): Guard = {
        guardMap.getOrElse(id, emptyGuard)
    }

    override def openGuard(id: String): Unit = {
        if (guardMap.contains(id) && !guardMap(id).isOpen) {
            guardMap(id).init()
            RootLogger.logger.info(s"start guard $id")
            executorService.execute(guardMap(id))
        }
    }

    def close(id: String): Unit = {
        val name = guardMap.keys.find(x => x.split("#").contains(id)).getOrElse("")
        val guard = getGuard(name)
        if (guard.isOpen) {
            guard.close()
        }
        guardMap = guardMap -- List(id)
    }

    def clean(): Unit = {
        guardMap.values.foreach(x => if (x.isOpen) x.close())
        guardMap = Map()
    }

    override def stopGuard(id: String): Unit = {
        guardMap.getOrElse(id, emptyGuard).stop()
    }

    override def startGuard(id: String): Unit = {
        guardMap.getOrElse(id, emptyGuard).start()
    }
}
