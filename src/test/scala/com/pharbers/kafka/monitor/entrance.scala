package com.pharbers.kafka.monitor

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.pharbers.kafka.producer.PharbersKafkaProducer
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
                   |        "topic": "${jobID}-oss-connect-test",
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
                   |        "topics": "${jobID}-oss-connect-test",
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


    //step 4 向MonitorServer拉取进度和处理情况（Kafka Consumer）（前提要确保MonitorServer已经启动!）


    //Step ？ 结束或发生异常时删除管道
    def deleteConnectors(jobID: String): Unit = {
        val deleteSourceConnectorResult = Http(monitor_config_obj.CONNECTOR_URL + "/" + s"${jobID}-oss-source-connector").method("DELETE").asString
        println(deleteSourceConnectorResult)
        val deleteSinkConnectorResult = Http(monitor_config_obj.CONNECTOR_URL + "/" + s"${jobID}--hdfs-sink-connector").method("DELETE").asString
        println(deleteSinkConnectorResult)
    }


//    val fu = PharbersKafkaProducer.apply.produce(requestTopic, jobID, "aha1024".getBytes)
//    println(fu.get(10, TimeUnit.SECONDS))


}



