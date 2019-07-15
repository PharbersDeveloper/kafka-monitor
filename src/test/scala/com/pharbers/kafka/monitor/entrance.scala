package com.pharbers.kafka.monitor

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.pharbers.kafka.consumer.PharbersKafkaConsumer
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
object entrance extends App {

    //传参
    val jobID: String = UUID.randomUUID().toString
    val excelFile: String = "oss-test.csv"
    var listenMonitor: Boolean = false
    println(s"START JOB ${jobID}")

    //step 1 创建source管道 ()
    def createSourceConnector(): Unit = {
        val createSourceConnectorResult = Http(monitor_config_obj.CONNECTOR_URL)
            .postData(
                s"""
                   |{
                   |    "name": "${jobID}-oss-source-connector",
                   |    "config": {
                   |        "connector.class": "com.pharbers.kafka.connect.oss.OssSourceConnector",
                   |        "tasks.max": "1",
                   |        "topic": "${jobID}-topic",
                   |        "endpoint": "oss-cn-beijing.aliyuncs.com",
                   |        "accessKeyId": "LTAIEoXgk4DOHDGi",
                   |        "accessKeySecret": "x75sK6191dPGiu9wBMtKE6YcBBh8EI",
                   |        "bucketName": "pharbers-resources",
                   |        "key": "${excelFile}"
                   |    }
                   |}
            """.stripMargin)
            .header("Content-Type", "application/json")
            .option(HttpOptions.readTimeout(10000))
            .asString
        println(createSourceConnectorResult)
    }
    //step 2 创建sink管道
    def createSinkConnector(jobID: String): Unit = {
        val createSourceConnectorResult = Http(monitor_config_obj.CONNECTOR_URL)
            .postData(
                s"""
                   |{
                   |    "name": "${jobID}-hdfs-sink-connector",
                   |    "config": {
                   |        "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
                   |        "tasks.max": "1",
                   |        "topics": "${jobID}-topic",
                   |        "hdfs.url": "hdfs://192.168.100.137:9000/logs/testLogs/",
                   |    	"flush.size": "3"
                   |    }
                   |}
            """.stripMargin)
            .header("Content-Type", "application/json")
            .option(HttpOptions.readTimeout(10000))
            .asString
        println(createSourceConnectorResult)
    }
    //step 3 向MonitorServer发送这次JobID的监控请求（Kafka Producer）（前提要确保MonitorServer已经启动!）
    // 请求参数（[JobID]和[监控策略]）
    def sendMonitorRequest(jobID: String): Unit = {
        val pkp = new PharbersKafkaProducer[String, MonitorRequest]
        val record = new MonitorRequest(jobID, "default")
        val fu = pkp.produce(monitor_config_obj.REQUEST_TOPIC, jobID, record)
        println(fu.get(10, TimeUnit.SECONDS))
    }


    //step 4 向MonitorServer拉取进度和处理情况（Kafka Consumer）（前提要确保MonitorServer已经启动!）
    def pollMonitorProgress(jobID: String): Unit = {
        listenMonitor = true
        val pkc = new PharbersKafkaConsumer[String, MonitorResponse](List(monitor_config_obj.RESPONSE_TOPIC), 1000, Int.MaxValue, myProcess)
        val t = new Thread(pkc)

        try {
            println("PollMonitorProgress starting!")
            t.start()

            println("PollMonitorProgress is started! Close by enter \"exit\" in console.")
            var cmd = Console.readLine()
            while (cmd != "exit") {
                cmd = Console.readLine()
            }

        } catch {
            case ie: InterruptedException => {
                println(ie.getMessage)
                t.stop()
                deleteConnectors(jobID)
            }
        } finally {
            t.stop()
            deleteConnectors(jobID)
            println("PollMonitorProgress close!")
        }
    }

    def myProcess[String, MonitorResponse](record: ConsumerRecord[String, MonitorResponse]): Unit = {
        println("===myProcess>>>" + record.key() + ":" + record.value().toString)
    }


    //Step ？ 结束或发生异常时删除管道
    def deleteConnectors(jobID: String): Unit = {
        val deleteSourceConnectorResult = Http(monitor_config_obj.CONNECTOR_URL + "/" + s"${jobID}-oss-source-connector").method("DELETE").asString
        println(deleteSourceConnectorResult)
        val deleteSinkConnectorResult = Http(monitor_config_obj.CONNECTOR_URL + "/" + s"${jobID}--hdfs-sink-connector").method("DELETE").asString
        println(deleteSinkConnectorResult)
    }

}



