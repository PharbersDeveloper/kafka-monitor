package com.pharbers.kafka.monitor.guard

import java.time.Duration
import java.util.concurrent.locks.{Condition, ReentrantLock}
import java.util.{Date, UUID}

import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.monitor.action.Action
import com.pharbers.kafka.monitor.manager.BaseGuardManager
import com.pharbers.kafka.monitor.util.RootLogger
import com.pharbers.kafka.schema.SinkRecall
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.logging.log4j.LogManager

import scala.collection.JavaConverters._

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/08/27 10:22
  * @note 一些值得注意的地方
  */
case class ConnectorGuard(connectorName: String, sourceTopic: String, recallTopic: String, action: Action, version: String = "") extends Guard {
    private val logger = LogManager.getLogger(this.getClass)
    private val lock = new ReentrantLock(true)
    private val aCondition: Condition = lock.newCondition
    //状态
    private var open = false
    private var sourceCount = 0L
    private var sinkCount = 0L
    private var canCheck = false

    override def init(): Unit = {
    }

    override def run(): Unit = {
        action.start()
        open = true
        logger.info(s"$connectorName; 开始guard")
        val sourceConsumer = new PharbersKafkaConsumer(Nil).getConsumer
        val sinkConsumer = new PharbersKafkaConsumer[String, SinkRecall](Nil).getConsumer
        try {
            while (isOpen){
                rateOfProgressListenner(sourceConsumer, sinkConsumer)
                canCheck = false
                stop()
                //等待外部restart
            }
        } finally {
            sourceConsumer.close()
            sinkConsumer.close()
        }
        close()
    }

    override def start(): Unit = {
        lock.lock()
        aCondition.signalAll()
        lock.unlock()
    }

    override def stop(): Unit = {
        lock.lock()
        aCondition.await()
        lock.unlock()
    }

    override def close(): Unit = {
        if (!open) {
            logger.info("已经关闭过了")
            return
        }
        action.end()
        logger.info(s"$jobId,关闭countGuard")
        open = false
    }

    override def isOpen: Boolean = {
        open
    }

    private def rateOfProgressListenner(sourceConsumer: KafkaConsumer[Nothing, Nothing], sinkConsumer: KafkaConsumer[String, SinkRecall]): Unit = {
        //todo： 配置
        var shouldTrueCount = 10
        var CanErrorCount = 10

        logger.info(s"$connectorName; 开始query")
        sinkConsumer.subscribe(List(recallTopic).asJava)
        while (isOpen && shouldTrueCount != 0 && CanErrorCount != 0) {
            logger.debug(s"isopen: $open")
            val resSourceCount = sourceConsumer
                    .endOffsets(sourceConsumer.partitionsFor(sourceTopic).asScala.map(x => new TopicPartition(x.topic(), x.partition())).asJava).asScala
                    .values.foldLeft(0L)(_ + _)
            logger.debug(s"获取sink recall count")
            val resSinkCount = sinkConsumer.poll(Duration.ofMillis(50))
                    .asScala.foldLeft(sinkCount)((left, right) => if (right.value().getCount >= left) right.value().getCount else left)
            if (resSourceCount != sourceCount || resSinkCount != sinkCount) {
                sourceCount = resSourceCount
                sinkCount = resSinkCount
                shouldTrueCount = 10
                canCheck = true
            } else {
                Thread.sleep(50)
            }
            try {
                if (checkCount(sourceCount, sinkCount, shouldTrueCount)) {
                    shouldTrueCount = shouldTrueCount - 1
                    logger.debug(s"$connectorName; 还差${shouldTrueCount}次相等")
                }
            } catch {
                case e: Exception =>
                    logger.error(s"$connectorName; 比较时发生错误, msg：$e")
                    CanErrorCount = CanErrorCount - 1
            }
        }
        if (shouldTrueCount == 0) {
            logger.info(s"$connectorName; 相等次数到10次")
            action.runTime("100")
        }
        if (CanErrorCount == 0) {
            logger.error(s"$connectorName; 错误次数到10次")
            action.error("错误次数到10次")
        }
    }

    private def checkCount(sourceCount: Long, sinkCount: Long, trueCount: Int): Boolean = {
        if (sourceCount == sinkCount && canCheck) {
            true
        } else {
            logger.debug(s"$connectorName; sinkCount: $sinkCount; sourceCount: $sourceCount")
            false
        }
    }

}
