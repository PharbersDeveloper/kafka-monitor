package com.pharbers.kafka.monitor.guard

import java.io.BufferedReader
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.{Date, UUID}

import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.monitor.action.Action
import com.pharbers.kafka.monitor.exception.HttpRequestException
import com.pharbers.kafka.monitor.httpClient.JsonMode.QueryMode
import com.pharbers.kafka.monitor.manager.BaseGuardManager
import com.pharbers.kafka.monitor.util.{JsonHandler, KsqlRunner, RootLogger}
import com.pharbers.kafka.schema.SinkRecall
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import org.apache.logging.log4j.LogManager

import scala.collection.JavaConverters._

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/08/18 11:21
  * @note 一些值得注意的地方
  */
case class TmGuard(jobId: String, action: Action, version: String = "") extends Guard {
    private var open = false
    private val guardId = if(version == "") jobId else version
    private val logger = LogManager.getLogger(this.getClass)

    override def init(): Unit = {
    }

    override def run(): Unit = {
        open = true
        action.start()
        var sourceCount = -1L
        var sinkCount = 0L
        //todo： 配置
        var shouldTrueCount = 10
        var CanErrorCount = 10

        val startTime = new Date().getTime
        logger.info(s"$jobId; 开始query")
        val sourceConsumer = new PharbersKafkaConsumer(Nil).getConsumer
        val sinkConsumer = new PharbersKafkaConsumer[String, SinkRecall](Nil).getConsumer
        sinkConsumer.subscribe(List(s"recall_$jobId").asJava)
        try {
            while (isOpen && shouldTrueCount != 0 && CanErrorCount != 0) {
                logger.debug(s"isopen: $open")
                val resSourceCount = sourceConsumer
                        .endOffsets(sourceConsumer.partitionsFor(s"source_$jobId").asScala.map(x => new TopicPartition(x.topic(), x.partition())).asJava).asScala
                        .values.foldLeft(0L)(_ + _)
                logger.debug(s"获取sink recall count")
                val resSinkCount = getCount(sinkConsumer.poll(Duration.ofMillis(50)), sinkCount)
                if(resSourceCount != sourceCount || resSinkCount != sinkCount){
                    sourceCount = resSourceCount
                    sinkCount = resSinkCount
                    shouldTrueCount = 10
                } else {
                    Thread.sleep(50)
                }
                try {
                    if (checkCount(sourceCount, sinkCount, shouldTrueCount)) {
                        shouldTrueCount = shouldTrueCount - 1
                        logger.debug(s"$jobId; 还差${shouldTrueCount}次相等")
                    }
                } catch {
                    case e: Exception =>
                        logger.error(s"$jobId; 比较时发生错误, msg：$e")
                        CanErrorCount = CanErrorCount - 1
                }
                speedCheck(startTime, (sourceCount + sinkCount) / 2)
            }
        } finally {
            sourceConsumer.close()
            sinkConsumer.close()
        }
        if (shouldTrueCount == 0) {
            action.runTime("100")
        }
        if (CanErrorCount == 0) {
            logger.error(s"$jobId; 错误次数到10次")
            action.error("错误次数到10次")
        }
        close()
    }

    override def close(): Unit = {
        if(!open) {
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

    private def getCount(records: ConsumerRecords[String, SinkRecall], count: Long): Long = {
        records.asScala.foldLeft(count)((left, right) => if (right.value().getCount >= left) right.value().getCount else left)
    }

    private def checkCount(sourceCount: Long, sinkCount: Long, trueCount: Int): Boolean = {
        if (sourceCount == sinkCount && sourceCount != 0) {
            action.runTime("99")
            true
        } else {
            logger.debug(s"$jobId; sinkCount: $sinkCount; sourceCount: $sourceCount")
            if (sinkCount > sourceCount) {
                action.runTime((1 / (trueCount + 1).toDouble * sourceCount / sinkCount * 100).toInt.toString)
                false
            } else {
                action.runTime((1 / (trueCount + 1).toDouble * (sinkCount + 1) / sourceCount * 100).toInt.toString)
                false
            }
        }
    }

    private def speedCheck(startTime: Long, count: Long): Unit ={
        val checkTime = (new Date().getTime - startTime) / 1000
        val countAvgOfSecond = Math.abs(count + 1) / (checkTime + 1)
        logger.debug(s"$jobId, 运行时间：$checkTime， speed: $countAvgOfSecond")
        if(countAvgOfSecond < 283 && checkTime > 60){
            restart()
        }
    }

    private def restart(): Unit = {
        if(version == ""){
            BaseGuardManager.close(jobId)
            val restartVersion =UUID.randomUUID().toString.replaceAll("-", "")
            logger.info(s"$jobId,guard超时未完成，开启一个新的， id: $restartVersion")
            try {
                val restartAction = action.cloneAction()
                BaseGuardManager.createGuard(jobId, TmGuard(jobId, restartAction, restartVersion))
                BaseGuardManager.openGuard(jobId)
            } catch {
                case e: Exception => RootLogger.logger.error(s"jobid: $jobId, id: $guardId, 重启监控失败", e)
            }
        }else{
            action.error("任务失败， source和sink结果有差异")
            BaseGuardManager.close(jobId)
        }
    }
}
