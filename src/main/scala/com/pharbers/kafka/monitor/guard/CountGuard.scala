package com.pharbers.kafka.monitor.guard

import java.io.BufferedReader
import java.net.SocketTimeoutException
import java.util.{Timer, TimerTask, UUID}

import com.pharbers.kafka.monitor.action.Action
import com.pharbers.kafka.monitor.exception.HttpRequestException
import com.pharbers.kafka.monitor.httpClient.JsonMode.QueryMode
import com.pharbers.kafka.monitor.manager.BaseGuardManager
import com.pharbers.kafka.monitor.util.{JsonHandler, KsqlRunner, RootLogger}

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/07/11 11:05
  * @note 一些值得注意的地方
  */
case class CountGuard(jobId: String, url: String, action: Action, id: String = "") extends Guard {
    private var open = false
    private val sqlId = if(id == "") jobId else id
    //todo： 超时设置应该可配置化
    private val overTime: Long = 1000 * 60 * 30

    override def init(): Unit = {
        val createSourceStream = "create stream source_stream_" + sqlId + " with (kafka_topic = '" + s"source_$jobId" + "', value_format = 'avro');"
        val createSinkStream = "create stream sink_stream_" + sqlId + " with (kafka_topic = '" + s"recall_$jobId" + "', value_format = 'avro');"
        createStream(createSourceStream)
        createStream(createSinkStream)
    }

    override def run(): Unit = {
        action.start()
        open = true
        new Timer().schedule(new RestartGuard(jobId, action), overTime)
        var sourceCount = -1L
        //        var sinkCount = 0L
        var sinkCount = 0L
        var shouldTrueCount = 10
        var CanErrorCount = 10

        val selectSourceCount = s"select count(*) from source_stream_$sqlId group by jobId;"
        val selectSinkCount = s"select count from sink_stream_$sqlId;"
//todo：测试用，等待1分钟，等待source完成
        Thread.sleep(1000 * 60)
        RootLogger.logger.info(s"$jobId; 开始query")
        val sourceRead = createQuery(selectSourceCount)
        val sinkRead = createQuery(selectSinkCount)
        try {
            while (isOpen) {
                sourceCount = getCount(sourceRead, sourceCount)
                sinkCount = getCount(sinkRead, sinkCount)

                try {
                    if (checkCount(sourceCount, sinkCount, shouldTrueCount)) {
                        shouldTrueCount = shouldTrueCount - 1
                        RootLogger.logger.debug(s"$jobId; 还差${shouldTrueCount}次相等")
                    }
                } catch {
                    case e: Exception =>
                        RootLogger.logger.error(s"$jobId; 比较时发生错误, msg：$e")
                    //                    CanErrorCount = CanErrorCount - 1
                }
                if (shouldTrueCount == 0) {
                    action.runTime("100")
                    close()
                }
                if (CanErrorCount == 0) {
                    RootLogger.logger.error(s"$jobId; 错误次数到10次")
                    action.error("错误次数到10次")
                    close()
                }
            }
        } finally {
            sourceRead.close()
            sinkRead.close()
        }
    }

    override def close(): Unit = {
        //todo: 这儿不应该删除topic，应该有单独的topic删除
        val dropTables = List(s"drop stream source_stream_$sqlId delete topic;", s"drop stream sink_stream_$sqlId delete topic;")
        try {
            dropTables.map(x => KsqlRunner.runSql(x, s"$url/ksql", Map()))
        } catch {
            //可能未创建stream就关闭了
            case e: HttpRequestException => RootLogger.logger.info(e.getMessage)
            case e: Exception =>
                RootLogger.logger.error("删除创建的ksql资源时发生未知错误", e)
                throw e
        }
        action.end()
        open = false
        //todo: 测试用，因为只有一个监控，所以直接关闭全部,之后应该只关闭相关的
        BaseGuardManager.closeAll()
    }

    override def isOpen: Boolean = {
        open
    }

    private def createStream(ksqlDDL: String): Unit = {
        try {
            val createSourceStreamResponse = KsqlRunner.runSql(ksqlDDL, s"$url/ksql", Map("ksql.streams.auto.offset.reset" -> "earliest"))
            RootLogger.logger.info(s"$jobId; ${createSourceStreamResponse.readLine()}")
        } catch {
            case e: HttpRequestException =>
                close()
                RootLogger.logger.error(s"$jobId; create stream error: ${e.getMessage}, sql: $ksqlDDL")
                action.error(s"create stream error: ${e.getMessage}")
                throw e
            case e: Exception =>
                close()
                RootLogger.logger.error(s"$jobId; 未知错误: $e")
                action.error(s"未知错误: $e")
                throw e
        }
    }

    private def createQuery(ksqlDML: String): BufferedReader = {
        try {
            KsqlRunner.runSql(ksqlDML, s"$url/query", Map("ksql.streams.auto.offset.reset" -> "earliest"))
            //            KsqlRunner.runSql(ksqlDML, s"$url/query", Map("ksql.streams.auto.offset.reset" -> "latest"))
        } catch {
            case e: HttpRequestException =>
                close()
                RootLogger.logger.error(s"$jobId; create query error: ${e.getMessage}, sql: $ksqlDML")
                action.error(s"ksql query error: ${e.getMessage}, url: $url, sql: $ksqlDML")
                throw e
            case e: SocketTimeoutException =>
                close()
                RootLogger.logger.error(s"$jobId; create query 连接超时: ${e.getMessage}, sql: $ksqlDML")
                action.error(s"ksql query 连接超时: ${e.getMessage}, url: $url, sql: $ksqlDML")
                throw e
            case e: Exception =>
                close()
                RootLogger.logger.error(s"$jobId; 未知错误: ${e.getMessage}")
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
                    RootLogger.logger.debug(e)
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
            RootLogger.logger.debug(s"$jobId; sinkCount: $sinkCount; sourceCount: $sourceCount")
            //测试用
            if (sinkCount > 178485 || sourceCount > 178485) {
                RootLogger.logger.debug(s"error: sink: $sinkCount; source: $sourceCount")
                //                throw new Exception(s"error:越界 sink: $sinkCount; source: $sourceCount")
            }
            if (sinkCount > sourceCount) {
                action.runTime((1 / (trueCount + 1).toDouble * sourceCount / sinkCount * 100).toInt.toString)
                false
            } else {
                action.runTime((1 / (trueCount + 1).toDouble * (sinkCount + 1) / sourceCount * 100).toInt.toString)
                false
            }
        }
    }

    class RestartGuard(jobId: String, action: Action) extends TimerTask {
        override def run(): Unit = {
            //todo: 这儿不能直接用BaseGuardManager，需要多态，按配置使用不同的GuardManager
            val guard = BaseGuardManager.getGuard(sqlId)
            if (guard.isOpen) {
                val id = UUID.randomUUID().toString.replaceAll("-", "")
                RootLogger.logger.info(s"$id,guard超时未完成，开启一个新的")
                BaseGuardManager.createGuard(id, CountGuard(jobId, url, action, id))
                BaseGuardManager.openGuard(id)
            }
        }
    }

}
