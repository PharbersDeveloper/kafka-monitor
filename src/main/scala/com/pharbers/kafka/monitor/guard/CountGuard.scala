package com.pharbers.kafka.monitor.guard

import java.io.BufferedReader
import java.net.SocketTimeoutException
import java.util.{Date, Timer, TimerTask, UUID}

import com.pharbers.kafka.monitor.action.Action
import com.pharbers.kafka.monitor.exception.HttpRequestException
import com.pharbers.kafka.monitor.httpClient.JsonMode.QueryMode
import com.pharbers.kafka.monitor.manager.BaseGuardManager
import com.pharbers.kafka.monitor.util.{JsonHandler, KsqlRunner, RootLogger}
import org.apache.logging.log4j.LogManager

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/11 11:05
  * @note 一些值得注意的地方
  */

//这是建立在topic中都是同一个key的监控
case class CountGuard(jobId: String, url: String, action: Action, version: String = "") extends Guard {
    private var open = false
    private val guardId = if(version == "") jobId else version
    private val logger = LogManager.getLogger(this.getClass)

    override def init(): Unit = {
        val createSourceStream = "create stream source_stream_" + guardId + " with (kafka_topic = '" + s"source_$jobId" + "', value_format = 'avro');"
        val createSinkStream = "create stream sink_stream_" + guardId + " with (kafka_topic = '" + s"recall_$jobId" + "', value_format = 'avro');"
        createStream(createSourceStream)
        createStream(createSinkStream)
    }

    override def run(): Unit = {
        open = true
        action.start()
        var sourceCount = -1L
        var sinkCount = 0L
        var shouldTrueCount = 10
        var CanErrorCount = 10

        val selectSourceCount = s"select count(*) from source_stream_$guardId group by rowkey;"
        val selectSinkCount = s"select count from sink_stream_$guardId;"
//todo：测试用，等待1分钟，等待source完成
//        Thread.sleep(1000 * 60)
        val startTime = new Date().getTime
        logger.info(s"$jobId; 开始query")
        val sourceRead = createQuery(selectSourceCount)
        val sinkRead = createQuery(selectSinkCount)
        try {
            while (isOpen && shouldTrueCount != 0 && CanErrorCount != 0) {
                logger.debug(s"isopen: $open")
                val resSourceCount = getCount(sourceRead, sourceCount)
                val resSinkCount = getCount(sinkRead, sinkCount)
                if(resSourceCount != sourceCount || resSinkCount != sinkCount){
                    sourceCount = resSourceCount
                    sinkCount = resSinkCount
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
            sourceRead.close()
            sinkRead.close()
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
        val dropTables = List(s"drop stream source_stream_$guardId;", s"drop stream sink_stream_$guardId;")
        try {
            //todo: 异步执行比较好
            dropTables.map(x => KsqlRunner.runSql(x, s"$url/ksql", Map(), 1))
        } catch {
            //可能未创建stream就关闭了
            case e: HttpRequestException => logger.info(e.getMessage)
            case e: Exception =>
                logger.error("删除创建的ksql资源时发生未知错误", e)
                throw e
        }
        action.end()
        logger.info(s"$jobId,关闭countGuard")
        open = false
    }

    override def isOpen: Boolean = {
        open
    }

    private def createStream(ksqlDDL: String): Unit = {
        try {
            val createSourceStreamResponse = KsqlRunner.runSql(ksqlDDL, s"$url/ksql", Map("ksql.streams.auto.offset.reset" -> "earliest"))
            logger.info(s"$jobId; ${createSourceStreamResponse.readLine()}")
        } catch {
            case e: HttpRequestException =>
                logger.error(s"$jobId; create stream error: ${e.getMessage}, sql: $ksqlDDL")
                action.error(s"create stream error: ${e.getMessage}")
                close()
                throw e
            case e: Exception =>
                logger.error(s"$jobId; 未知错误: $e")
                action.error(s"未知错误: $e")
                close()
                throw e
        }
    }

    private def createQuery(ksqlDML: String): BufferedReader = {
        try {
            KsqlRunner.runSql(ksqlDML, s"$url/query", Map("ksql.streams.auto.offset.reset" -> "earliest"))
        } catch {
            case e: HttpRequestException =>
                close()
                logger.error(s"$jobId; create query error: ${e.getMessage}, sql: $ksqlDML")
                action.error(s"ksql query error: ${e.getMessage}, url: $url, sql: $ksqlDML")
                throw e
            case e: SocketTimeoutException =>
                close()
                logger.error(s"$jobId; create query 连接超时: ${e.getMessage}, sql: $ksqlDML")
                action.error(s"ksql query 连接超时: ${e.getMessage}, url: $url, sql: $ksqlDML")
                throw e
            case e: Exception =>
                close()
                logger.error(s"$jobId; 未知错误: ${e.getMessage}")
                action.error(s"未知错误: ${e.getMessage}")
                throw e
        }
    }

    private def getCount(reader: BufferedReader, count: Long): Long = {
        val json = if (reader.ready()) {
            reader.readLine
        } else {
            ""
        }
        if (json.length > 0) {
            val row = JsonHandler.readObject[QueryMode](json).row
            try {
                row.getColumns.get(0).toLong
            } catch {
                case e: Exception =>
                    logger.debug(e)
                    count
            }
        } else {
            count
        }
    }

    private def checkCount(sourceCount: Long, sinkCount: Long, trueCount: Int): Boolean = {
        if (sourceCount == sinkCount) {
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
        if(countAvgOfSecond < 283 && checkTime > 120){
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
                BaseGuardManager.createGuard(jobId, CountGuard(jobId, url, restartAction, restartVersion))
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
