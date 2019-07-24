package com.pharbers.kafka.monitor

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.monitor.util.RootLogger
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.{MonitorRequest, MonitorResponse}
import org.apache.kafka.clients.consumer.ConsumerRecord
import scalaj.http.{Http, HttpOptions}

/**
  * @ ProjectName kafka-monitor.com.pharbers.kafka.monitor.entrance
  * @ author jeorch
  * @ date 19-7-12
  * @ Description: TODO
  */
object entrance2 extends App {

    //传参
    var jobID: String = ""
    val hdfsPath: String = "hdfs://192.168.100.137:9000/logs/testLogs/parquet/topics/test_005/partition=0/"
    val esUrl: String = "http://192.168.100.157:9200"
    var listenMonitor: Boolean = false

    (1 to 1).foreach(x => {
        RootLogger.logger.info(s"第${x}次")
        jobID = UUID.randomUUID().toString.replaceAll("-", "")
        RootLogger.logger.info(s"START JOB ${jobID}")
        createSourceConnector()
        createSinkConnector()
        sendMonitorRequest()
        pollMonitorProgress(jobID)
    }
    )


    //step 1 创建source管道 ()
    def createSourceConnector(): Unit = {
        val createSourceConnectorResult = Http(monitor_config_obj.CONNECTOR_URL)
                .postData(
                    s"""
                       |{
                       |    "name": "${jobID}-hdfs-source-connector",
                       |    "config": {
                       |        "connector.class": "com.github.mmolimar.kafka.connect.fs.FsSourceConnector",
                       |        "tasks.max": "1",
                       |        "policy.class": "com.github.mmolimar.kafka.connect.fs.policy.SimplePolicy",
                       |        "policy.recursive": "true",
                       |        "policy.regexp": ".*",
                       |        "file_reader.class": "com.github.mmolimar.kafka.connect.fs.file.reader.ParquetFileReader",
                       |        "fs.uris": "${hdfsPath}",
                       |        "jobId": "${jobID}",
                       |        "topic": "source_${jobID}"
                       |    }
                       |}
            """.stripMargin)
                .header("Content-Type", "application/json")
                .option(HttpOptions.readTimeout(10000))
                .asString
        RootLogger.logger.info(createSourceConnectorResult)
    }

    //step 2 创建sink管道
    def createSinkConnector(): Unit = {
        val createSourceConnectorResult = Http(monitor_config_obj.CONNECTOR_URL)
                .postData(
                    s"""
                       |{
                       |    "name": "${jobID}-es-sink-connector",
                       |    "config": {
                       |        "topics": "source_${jobID}",
                       |        "jobId": "${jobID}",
                       |        "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
                       |		"tasks.max": "1",
                       |		"key.ignore": "true",
                       |		"connection.url": "${esUrl}",
                       |		"type.name": ""
                       |    }
                       |}
            """.stripMargin)
                .header("Content-Type", "application/json")
                .option(HttpOptions.readTimeout(10000))
                .asString
        RootLogger.logger.info(createSourceConnectorResult)
    }

    //step 3 向MonitorServer发送这次JobID的监控请求（Kafka Producer）（前提要确保MonitorServer已经启动!）
    // 请求参数（[JobID]和[监控策略]）
    def sendMonitorRequest(): Unit = {
        val pkp = new PharbersKafkaProducer[String, MonitorRequest]
        val record = new MonitorRequest(jobID, "default")
        val fu = pkp.produce(monitor_config_obj.REQUEST_TOPIC, jobID, record)
        RootLogger.logger.info(fu.get(10, TimeUnit.SECONDS))
    }


    //step 4 向MonitorServer拉取进度和处理情况（Kafka Consumer）（前提要确保MonitorServer已经启动!）
    def pollMonitorProgress(jobID: String): Unit = {
        var time = 0
        listenMonitor = true
        val pkc = new PharbersKafkaConsumer[String, MonitorResponse](List(monitor_config_obj.RESPONSE_TOPIC), 1000, Int.MaxValue, myProcess)
        val t = new Thread(pkc)

        try {
            RootLogger.logger.info("PollMonitorProgress starting!")
            t.start()

            RootLogger.logger.info("PollMonitorProgress is started! Close by enter \"exit\" in console.")
//            var cmd = Console.readLine()
            while (listenMonitor) {
                Thread.sleep(30000)
                time = time + 1
                if (time > 50) {
                    RootLogger.logger.error("error: 程序异常")
                    listenMonitor = false
                }
            }

        } catch {
            case ie: InterruptedException => {
                RootLogger.logger.error(ie.getMessage)
                pkc.close()
                deleteConnectors(jobID)
            }
        } finally {
            pkc.close()
            deleteConnectors(jobID)
            RootLogger.logger.info("PollMonitorProgress close!")
        }
    }

    def myProcess(record: ConsumerRecord[String, MonitorResponse]): Unit = {
        RootLogger.logger.info("===myProcess>>>" + record.key() + ":" + record.value().toString)
        if (record.value().getProgress == 100 && record.value().getJobId.toString == jobID) {
            listenMonitor = false
            deleteConnectors(record.value().getJobId.toString)
        }
        if(record.value().getError.toString != ""){
            RootLogger.logger.info(s"收到错误信息后关闭，id: ${record.value().getJobId.toString}, error：${record.value().getError.toString}")
        }
    }


    //Step ？ 结束或发生异常时删除管道
    def deleteConnectors(jobID: String): Unit = {
        try{
            val deleteSourceConnectorResult = Http(monitor_config_obj.CONNECTOR_URL + "/" + s"${jobID}-hdfs-source-connector").method("DELETE").asString
            RootLogger.logger.info(deleteSourceConnectorResult)
            val deleteSinkConnectorResult = Http(monitor_config_obj.CONNECTOR_URL + "/" + s"${jobID}-es-sink-connector").method("DELETE").asString
            RootLogger.logger.info(deleteSinkConnectorResult)
        } catch {
            case e: Exception =>  RootLogger.logger.error(e.getMessage)
        }

    }


}



