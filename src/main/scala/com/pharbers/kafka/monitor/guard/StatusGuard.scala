package com.pharbers.kafka.monitor.guard

import java.net.InetAddress
import java.time.Duration
import java.util.Properties

import com.pharbers.kafka.common.kafka_config_obj
import com.pharbers.kafka.monitor.Config
import com.pharbers.kafka.monitor.action.Action
import com.pharbers.kafka.monitor.manager.GuardManager
import com.pharbers.kafka.monitor.util.JsonHandler
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.logging.log4j.LogManager

import scala.collection.JavaConverters._

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/08/27 11:12
  * @note 一些值得注意的地方
  */
case class StatusGuard(jobId: String, action: Action, version: String = "") extends Guard {
    private val logger = LogManager.getLogger(this.getClass)
    private var open = false
    override def init(): Unit = {
    }

    override def run(): Unit = {
        open = true
        action.start()
        val consumer = getConsumer
        consumer.subscribe(List(Config.config.get("statusTopic").asText()).asJava)
        while (isOpen){
            val records = consumer.poll(Duration.ofMillis(1000))
            records.asScala.filter(x => x.value() != null).foreach(x => {
                logger.info(s"key:${x.key()}, value:${x.value()}")
                val connectorName = getName(x.key())
                val value = JsonHandler.objectMapper.readTree(x.value())
                value.get("state").asText() match {
                    case "RUNNING" => GuardManager.startGuard(connectorName)
                    case "UNASSIGNED" => logger.info(s"key:${x.key()} UNASSIGNED")
                    case "FAILED" =>
                        action.error(connectorName + "#" +value.get("trace").asText())
                        GuardManager.close(connectorName)
                    case "PAUSED" => logger.info(s"key:${x.key()} PAUSED")
                }
            })
        }
        consumer.close(Duration.ofSeconds(10))
    }

    override def close(): Unit = {
        if(!open) {
            logger.info("已经关闭过了")
            return
        }
        action.end()
        logger.info(s"关闭statusGuard")
        open = false
    }

    override def isOpen: Boolean = {
        open
    }

    private def getConsumer: KafkaConsumer[String, String] ={
        val config = new Properties()
        config.put("client.id", InetAddress.getLocalHost.getHostName)
        config.put("group.id", kafka_config_obj.group)
        config.put("bootstrap.servers", kafka_config_obj.broker)
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        config.put("security.protocol", kafka_config_obj.securityProtocol)
        config.put("ssl.endpoint.identification.algorithm", kafka_config_obj.sslAlgorithm)
        config.put("ssl.truststore.location", kafka_config_obj.sslTruststoreLocation)
        config.put("ssl.truststore.password", kafka_config_obj.sslTruststorePassword)
        config.put("ssl.keystore.location", kafka_config_obj.sslKeystoreLocation)
        config.put("ssl.keystore.password", kafka_config_obj.sslKeystorePassword)
        new KafkaConsumer[String, String](config)
    }

    private def getName(key: String): String ={
        val keys = key.split("-")
        if (keys.length > 2){
            keys(1) match {
                case "connector" => keys.tail.tail.mkString("")
                case "task" => keys.take(keys.length - 1).tail.tail.mkString("")
            }

        }else{
            ""
        }
    }
}
